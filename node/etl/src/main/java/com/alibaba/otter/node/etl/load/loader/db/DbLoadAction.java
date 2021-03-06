/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.otter.node.etl.load.loader.db;

import com.alibaba.otter.node.etl.common.db.dialect.DbDialect;
import com.alibaba.otter.node.etl.common.db.dialect.DbDialectFactory;
import com.alibaba.otter.node.etl.common.db.dialect.mysql.MysqlDialect;
import com.alibaba.otter.node.etl.common.db.utils.SqlUtils;
import com.alibaba.otter.node.etl.load.exception.LoadException;
import com.alibaba.otter.node.etl.load.loader.AbstractLoadAction;
import com.alibaba.otter.node.etl.load.loader.common.AbstractLoadWorker;
import com.alibaba.otter.node.etl.load.loader.common.DataLoadContext;
import com.alibaba.otter.node.etl.load.loader.common.LoadAction;
import com.alibaba.otter.node.etl.load.loader.common.LoadDataFilter;
import com.alibaba.otter.node.etl.load.loader.db.DbLoadData.TableLoadData;
import com.alibaba.otter.node.etl.load.loader.db.context.DbLoadContext;
import com.alibaba.otter.shared.common.model.config.ConfigHelper;
import com.alibaba.otter.shared.common.model.config.channel.Channel;
import com.alibaba.otter.shared.common.model.config.data.DataMedia;
import com.alibaba.otter.shared.common.model.config.data.DataMediaType;
import com.alibaba.otter.shared.common.model.config.data.db.DbMediaSource;
import com.alibaba.otter.shared.common.model.config.pipeline.Pipeline;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import com.alibaba.otter.shared.etl.model.EventType;
import com.alibaba.otter.shared.etl.model.Identity;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.support.lob.LobCreator;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.util.CollectionUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.Future;

/**
 * 数据库load的执行入口
 * 
 * @author jianghang 2011-10-31 下午03:17:43
 * @version 4.0.0
 */
@LoadAction(actionName = "dbLoadAction", mediaType = {DataMediaType.MYSQL, DataMediaType.ORACLE})
public class DbLoadAction extends AbstractLoadAction {

    private static final Logger logger             = LoggerFactory.getLogger(DbLoadAction.class);
    private static final String WORKER_NAME        = "DbLoadAction";
    private static final String WORKER_NAME_FORMAT = "pipelineId = %s , pipelineName = %s , " + WORKER_NAME;
    private int                 retry              = 3;
    private int                 retryWait          = 3000;
    private DbDialectFactory    dbDialectFactory;
    private int                 batchSize          = 50;
    private boolean             useBatch           = true;


    @Override
    protected void doDdl(DataLoadContext context, List<EventData> eventDatas) {
        doDdlLocal((DbLoadContext) context, eventDatas);
    }

    @Override
    protected void doDMLLoad(DataLoadContext context, List<EventData> eventDatas) {
        // 进行一次数据合并，合并相同pk的多次I/U/D操作
        List<EventData> items = DbLoadMerger.merge(eventDatas);
        // 按I/U/D进行归并处理
        final DbLoadData loadData = new DbLoadData();
        doBefore(items, context, new LoadDataFilter<EventData>() {
            @Override
            public void doFilter(EventData data) {
                loadData.merge(data);
            }
        });
        // 执行load操作
        doLoad((DbLoadContext) context, loadData);
    }

    public DbLoadContext buildContext(Identity identity) {
        DbLoadContext context = new DbLoadContext();
        context.setIdentity(identity);
        Channel channel = configClientService.findChannel(identity.getChannelId());
        Pipeline pipeline = configClientService.findPipeline(identity.getPipelineId());
        context.setChannel(channel);
        context.setPipeline(pipeline);
        return context;
    }

    private void doLoad(final DbLoadContext context, DbLoadData loadData) {
        // 优先处理delete,可以利用batch优化
        List<List<EventData>> batchDatas = new ArrayList<List<EventData>>();
        for (TableLoadData tableData : loadData.getTables()) {
            if (useBatch) {
                // 优先执行delete语句，针对uniqe更新，一般会进行delete + insert的处理模式，避免并发更新
                batchDatas.addAll(split(tableData.getDeleteDatas()));
            } else {
                // 如果不可以执行batch，则按照单条数据进行并行提交
                // 优先执行delete语句，针对uniqe更新，一般会进行delete + insert的处理模式，避免并发更新
                for (EventData data : tableData.getDeleteDatas()) {
                    batchDatas.add(Collections.singletonList(data));
                }
            }
        }

        if (context.getPipeline().getParameters().isDryRun()) {
            doDryRun(context, batchDatas, true);
        } else {
            doTwoPhase(context, batchDatas, true);
        }
        batchDatas.clear();

        // 处理下insert/update
        for (TableLoadData tableData : loadData.getTables()) {
            if (useBatch) {
                // 执行insert + update语句
                batchDatas.addAll(split(tableData.getInsertDatas()));
                batchDatas.addAll(split(tableData.getUpadateDatas()));// 每条记录分为一组，并行加载
            } else {
                // 执行insert + update语句
                for (EventData data : tableData.getInsertDatas()) {
                    batchDatas.add(Collections.singletonList(data));
                }
                for (EventData data : tableData.getUpadateDatas()) {
                    batchDatas.add(Collections.singletonList(data));
                }
            }
        }

        if (context.getPipeline().getParameters().isDryRun()) {
            doDryRun(context, batchDatas, true);
        } else {
            doTwoPhase(context, batchDatas, true);
        }
        batchDatas.clear();
    }

    /**
     * 将对应的数据按照sql相同进行batch组合
     */
    private List<List<EventData>> split(List<EventData> datas) {
        List<List<EventData>> result = new ArrayList<List<EventData>>();
        if (datas == null || datas.size() == 0) {
            return result;
        } else {
            int[] bits = new int[datas.size()];// 初始化一个标记，用于标明对应的记录是否已分入某个batch
            for (int i = 0; i < bits.length; i++) {
                // 跳过已经被分入batch的
                while (i < bits.length && bits[i] == 1) {
                    i++;
                }

                if (i >= bits.length) { // 已处理完成，退出
                    break;
                }

                // 开始添加batch，最大只加入batchSize个数的对象
                List<EventData> batch = new ArrayList<EventData>();
                bits[i] = 1;
                batch.add(datas.get(i));
                for (int j = i + 1; j < bits.length && batch.size() < batchSize; j++) {
                    if (bits[j] == 0 && canBatch(datas.get(i), datas.get(j))) {
                        batch.add(datas.get(j));
                        bits[j] = 1;// 修改为已加入
                    }
                }
                result.add(batch);
            }

            return result;
        }
    }

    /**
     * 判断两条记录是否可以作为一个batch提交，主要判断sql是否相等. 可优先通过schemaName进行判断
     */
    private boolean canBatch(EventData source, EventData target) {
        // return StringUtils.equals(source.getSchemaName(),
        // target.getSchemaName())
        // && StringUtils.equals(source.getTableName(), target.getTableName())
        // && StringUtils.equals(source.getSql(), target.getSql());
        // return StringUtils.equals(source.getSql(), target.getSql());

        // 因为sqlTemplate构造sql时用了String.intern()的操作，保证相同字符串的引用是同一个，所以可以直接使用==进行判断，提升效率
        return source.getSql() == target.getSql();
    }

    /**
     * 执行ddl的调用，处理逻辑比较简单: 串行调用
     *
     * @param context 数据load处理上下文
     * @param eventDatas load事件数据
     */
    private void doDdlLocal(DbLoadContext context, List<EventData> eventDatas) {
        for (final EventData data : eventDatas) {
            DataMedia dataMedia = ConfigHelper.findDataMedia(context.getPipeline(), data.getTableId());
            final DbDialect dbDialect = dbDialectFactory.getDbDialect(context.getIdentity().getPipelineId(),
                    (DbMediaSource) dataMedia.getSource());
            Boolean skipDdlException = context.getPipeline().getParameters().getSkipDdlException();
            try {
                Boolean result = dbDialect.getJdbcTemplate().execute(new StatementCallback<Boolean>() {

                    public Boolean doInStatement(Statement stmt) throws SQLException, DataAccessException {
                        Boolean result = true;
                        if (dbDialect instanceof MysqlDialect && StringUtils.isNotEmpty(data.getDdlSchemaName())) {
                            // 如果mysql，执行ddl时，切换到在源库执行的schema上
                            // result &= stmt.execute("use " +
                            // data.getDdlSchemaName());

                            // 解决当数据库名称为关键字如"Order"的时候,会报错,无法同步
                            result &= stmt.execute("use `" + data.getDdlSchemaName() + "`");
                        }
                        result &= stmt.execute(data.getSql());
                        return result;
                    }
                });
                if (result) {
                    context.getProcessedDatas().add(data); // 记录为成功处理的sql
                } else {
                    context.getFailedDatas().add(data);
                }

            } catch (Throwable e) {
                if (skipDdlException) {
                    // do skip
                    logger.warn("skip exception for ddl : {} , caused by {}", data, ExceptionUtils.getFullStackTrace(e));
                } else {
                    throw new LoadException(e);
                }
            }

        }
    }

    /**
     * 首先进行并行执行，出错后转为串行执行
     */
    private void doTwoPhase(DbLoadContext context, List<List<EventData>> totalRows, boolean canBatch) {
        // 预处理下数据
        List<Future<Exception>> results = new ArrayList<Future<Exception>>();
        for (List<EventData> rows : totalRows) {
            if (CollectionUtils.isEmpty(rows)) {
                continue; // 过滤空记录
            }

            results.add(executor.submit(new DbLoadWorker(context, rows, canBatch)));
        }

        if (isPartFailed(context, totalRows, results)) {
            // if (CollectionUtils.isEmpty(context.getFailedDatas())) {
            // logger.error("##load phase one failed but failedDatas is empty!");
            // return;
            // }

            // 尝试的内容换成phase one跑的所有数据，避免因failed datas计算错误而导致丢数据
            List<EventData> retryEventDatas = new ArrayList<EventData>();
            for (List<EventData> rows : totalRows) {
                retryEventDatas.addAll(rows);
            }

            context.getFailedDatas().clear(); // 清理failed data数据

            // 可能为null，manager老版本数据序列化传输时，因为数据库中没有skipLoadException变量配置
            Boolean skipLoadException = context.getPipeline().getParameters().getSkipLoadException();
            if (skipLoadException != null && skipLoadException) {// 如果设置为允许跳过单条异常，则一条条执行数据load，准确过滤掉出错的记录，并进行日志记录
                for (EventData retryEventData : retryEventDatas) {
                    DbLoadWorker worker = new DbLoadWorker(context, Arrays.asList(retryEventData), false);// 强制设置batch为false
                    doCallableSkipException(worker, retryEventData);
                }
            } else {
                // 直接一批进行处理，减少线程调度
                DbLoadWorker worker = new DbLoadWorker(context, retryEventDatas, false);// 强制设置batch为false
                doCallableThrowException(worker);
            }

            // 清理failed data数据
            for (EventData data : retryEventDatas) {
                interceptor.after(context, data);// 通知加载完成
            }
        }

    }

    public void destroy() throws Exception {
        executor.shutdownNow();
    }

    class DbLoadWorker extends AbstractLoadWorker {

        private DbLoadContext   context;
        private DbDialect       dbDialect;
        private List<EventData> datas;
        private boolean         canBatch;

        public DbLoadWorker(DbLoadContext context, List<EventData> datas, boolean canBatch) {
            this.context = context;
            this.datas = datas;
            this.canBatch = canBatch;

            EventData data = datas.get(0); // eventData为同一数据库的记录，只取第一条即可
            DataMedia dataMedia = ConfigHelper.findDataMedia(context.getPipeline(), data.getTableId());
            dbDialect = dbDialectFactory.getDbDialect(context.getIdentity().getPipelineId(),
                    (DbMediaSource) dataMedia.getSource());
        }

        @Override
        public Exception call() throws Exception {
            try {
                Thread.currentThread().setName(String.format(WORKER_NAME_FORMAT,
                        context.getPipeline().getId(),
                        context.getPipeline().getName()));
                return doCall(context, datas, useBatch, canBatch, batchSize, retry, retryWait);
            } finally {
                Thread.currentThread().setName(WORKER_NAME);
            }
        }

        @Override
        public Map<ExecuteResult, LoadException> doCustomCall(final List<EventData> splitDatas) {

            Map<ExecuteResult, LoadException> result = new HashMap<ExecuteResult, LoadException>();

            try {
                final LobCreator lobCreator = dbDialect.getLobHandler().getLobCreator();
                if (useBatch && canBatch) {
                    // 处理batch
                    final String sql = splitDatas.get(0).getSql();
                    int[] affects = new int[splitDatas.size()];
                    affects = (int[]) dbDialect.getTransactionTemplate().execute(new TransactionCallback() {

                        public Object doInTransaction(TransactionStatus status) {
                            // 初始化一下内容
                            try {
                                interceptor.transactionBegin(context, splitDatas, dbDialect);
                                JdbcTemplate template = dbDialect.getJdbcTemplate();
                                int[] affects = template.batchUpdate(sql, new BatchPreparedStatementSetter() {

                                    public void setValues(PreparedStatement ps, int idx) throws SQLException {
                                        doPreparedStatement(ps, dbDialect, lobCreator, splitDatas.get(idx));
                                    }

                                    public int getBatchSize() {
                                        return splitDatas.size();
                                    }
                                });
                                interceptor.transactionEnd(context, splitDatas, dbDialect);
                                return affects;
                            } finally {
                                lobCreator.close();
                            }
                        }

                    });

                    // 更新统计信息
                    for (int i = 0; i < splitDatas.size(); i++) {
                        processStat(context, splitDatas.get(i), affects[i], true);
                    }
                } else {
                    final EventData data = splitDatas.get(0);// 直接取第一条
                    int affect = 0;
                    affect = (Integer) dbDialect.getTransactionTemplate().execute(new TransactionCallback() {

                        public Object doInTransaction(TransactionStatus status) {
                            try {
                                interceptor.transactionBegin(context, Arrays.asList(data), dbDialect);
                                JdbcTemplate template = dbDialect.getJdbcTemplate();
                                int affect = template.update(data.getSql(), new PreparedStatementSetter() {

                                    public void setValues(PreparedStatement ps) throws SQLException {
                                        doPreparedStatement(ps, dbDialect, lobCreator, data);
                                    }
                                });
                                interceptor.transactionEnd(context, Arrays.asList(data), dbDialect);
                                return affect;
                            } finally {
                                lobCreator.close();
                            }
                        }
                    });
                    // 更新统计信息
                    processStat(context, data, affect, false);
                }

                result.put(ExecuteResult.SUCCESS, null);
            } catch (DeadlockLoserDataAccessException ex) {
                result.put(ExecuteResult.RETRY,
                        new LoadException(ExceptionUtils.getFullStackTrace(ex),
                        DbLoadDumper.dumpEventDatas(splitDatas)));
            } catch (DataIntegrityViolationException ex) {
                result.put(ExecuteResult.ERROR, new LoadException(ExceptionUtils.getFullStackTrace(ex),
                        DbLoadDumper.dumpEventDatas(splitDatas)));
            } catch (RuntimeException ex) {
                result.put(ExecuteResult.ERROR, new LoadException(ExceptionUtils.getFullStackTrace(ex),
                        DbLoadDumper.dumpEventDatas(splitDatas)));
            } catch (Throwable ex) {
                result.put(ExecuteResult.ERROR, new LoadException(ExceptionUtils.getFullStackTrace(ex),
                        DbLoadDumper.dumpEventDatas(splitDatas)));
            }
            return result;
        }

        @Override
        public void processStat(EventData data, DataLoadContext context) {
            DbLoadAction.this.processStat(data, context);
        }

        private void doPreparedStatement(PreparedStatement ps, DbDialect dbDialect, LobCreator lobCreator,
                                         EventData data) throws SQLException {
            EventType type = data.getEventType();
            // 注意insert/update语句对应的字段数序都是将主键排在后面
            List<EventColumn> columns = new ArrayList<EventColumn>();
            if (type.isInsert()) {
                columns.addAll(data.getColumns()); // insert为所有字段
                columns.addAll(data.getKeys());
            } else if (type.isDelete()) {
                columns.addAll(data.getKeys());
            } else if (type.isUpdate()) {
                boolean existOldKeys = !CollectionUtils.isEmpty(data.getOldKeys());
                columns.addAll(data.getUpdatedColumns());// 只更新带有isUpdate=true的字段
                if (existOldKeys && dbDialect.isDRDS()) {
                    // DRDS需要区分主键是否有变更
                    columns.addAll(data.getUpdatedKeys());
                } else {
                    columns.addAll(data.getKeys());
                }
                if (existOldKeys) {
                    columns.addAll(data.getOldKeys());
                }
            }

            // 获取一下当前字段名的数据是否必填
            Table table = dbDialect.findTable(data.getSchemaName(), data.getTableName());
            Map<String, Boolean> isRequiredMap = new HashMap<String, Boolean>();
            for (Column tableColumn : table.getColumns()) {
                isRequiredMap.put(StringUtils.lowerCase(tableColumn.getName()), tableColumn.isRequired());
            }

            for (int i = 0; i < columns.size(); i++) {
                int paramIndex = i + 1;
                EventColumn column = columns.get(i);
                int sqlType = column.getColumnType();

                Boolean isRequired = isRequiredMap.get(StringUtils.lowerCase(column.getColumnName()));
                if (isRequired == null) {
                    // 清理一下目标库的表结构,二次检查一下
                    table = dbDialect.findTable(data.getSchemaName(), data.getTableName(), false);
                    isRequiredMap = new HashMap<String, Boolean>();
                    for (Column tableColumn : table.getColumns()) {
                        isRequiredMap.put(StringUtils.lowerCase(tableColumn.getName()), tableColumn.isRequired());
                    }

                    isRequired = isRequiredMap.get(StringUtils.lowerCase(column.getColumnName()));
                    if (isRequired == null) {
                        throw new LoadException(String.format("column name %s is not found in Table[%s]",
                                column.getColumnName(),
                                table.toString()));
                    }
                }

                Object param = null;
                if (dbDialect instanceof MysqlDialect
                        && (sqlType == Types.TIME || sqlType == Types.TIMESTAMP || sqlType == Types.DATE)) {
                    // 解决mysql的0000-00-00 00:00:00问题，直接依赖mysql
                    // driver进行处理，如果转化为Timestamp会出错
                    param = column.getColumnValue();
                } else {
                    param = SqlUtils.stringToSqlValue(column.getColumnValue(),
                            sqlType,
                            isRequired,
                            dbDialect.isEmptyStringNulled());
                }

                try {
                    switch (sqlType) {
                        case Types.CLOB:
                            lobCreator.setClobAsString(ps, paramIndex, (String) param);
                            break;

                        case Types.BLOB:
                            lobCreator.setBlobAsBytes(ps, paramIndex, (byte[]) param);
                            break;
                        case Types.TIME:
                        case Types.TIMESTAMP:
                        case Types.DATE:
                            // 只处理mysql的时间类型，oracle的进行转化处理
                            if (dbDialect instanceof MysqlDialect) {
                                // 解决mysql的0000-00-00 00:00:00问题，直接依赖mysql
                                // driver进行处理，如果转化为Timestamp会出错
                                ps.setObject(paramIndex, param);
                            } else {
                                StatementCreatorUtils.setParameterValue(ps, paramIndex, sqlType, null, param);
                            }
                            break;
                        case Types.BIT:
                            // 只处理mysql的bit类型，bit最多存储64位，所以需要使用BigInteger进行处理才能不丢精度
                            // mysql driver将bit按照setInt进行处理，会导致数据越界
                            if (dbDialect instanceof MysqlDialect) {
                                StatementCreatorUtils.setParameterValue(ps, paramIndex, Types.DECIMAL, null, param);
                            } else {
                                StatementCreatorUtils.setParameterValue(ps, paramIndex, sqlType, null, param);
                            }
                            break;
                        default:
                            StatementCreatorUtils.setParameterValue(ps, paramIndex, sqlType, null, param);
                            break;
                    }
                } catch (SQLException ex) {
                    logger.error("## SetParam error , [pairId={}, sqltype={}, value={}]",
                            new Object[] { data.getPairId(), sqlType, param });
                    throw ex;
                }
            }
        }

    }

    // =============== setter / getter ===============
    public void setRetry(int retry) {
        this.retry = retry;
    }

    public void setRetryWait(int retryWait) {
        this.retryWait = retryWait;
    }

    public void setDbDialectFactory(DbDialectFactory dbDialectFactory) {
        this.dbDialectFactory = dbDialectFactory;
    }
}