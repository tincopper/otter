package com.alibaba.otter.node.etl.load.loader.hdfs;

import com.alibaba.otter.node.etl.common.datasource.DataSourceService;
import com.alibaba.otter.node.etl.load.exception.LoadException;
import com.alibaba.otter.node.etl.load.loader.AbstractLoadAction;
import com.alibaba.otter.node.etl.load.loader.common.AbstractLoadWorker;
import com.alibaba.otter.node.etl.load.loader.common.DataLoadContext;
import com.alibaba.otter.node.etl.load.loader.common.LoadAction;
import com.alibaba.otter.node.etl.load.loader.common.LoadDataFilter;
import com.alibaba.otter.node.etl.load.loader.db.DbLoadDumper;
import com.alibaba.otter.shared.common.model.config.data.DataMediaType;
import com.alibaba.otter.shared.common.model.config.data.hdfs.HdfsDataSource;
import com.alibaba.otter.shared.common.utils.hdfs.HDFSUtils;
import com.alibaba.otter.shared.etl.model.EventData;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;

/**
 * hdfs数据加载
 * @author tangzhongyuan
 * @create 2018-09-12 10:16
 **/
@LoadAction(actionName = "hdfsLoadAction", mediaType = {DataMediaType.HDFS})
public class HdfsLoadAction extends AbstractLoadAction {

    private static final Logger logger = LoggerFactory.getLogger(HdfsLoadAction.class);

    private static final String WORKER_NAME        = "HdfsLoadAction";
    private static final String WORKER_NAME_FORMAT = "pipelineId = %s , pipelineName = %s , " + WORKER_NAME;
    private int                 retry              = 3;
    private int                 retryWait          = 3000;
    private DataSourceService   hdfsDataSourceService;
    private int batchSize                          = 30;

    @Override
    protected void doDdl(DataLoadContext context, List<EventData> eventDatas) {
        for (EventData data : eventDatas) {
            ddlForHdfs(context, data);
        }
    }

    @Override
    protected void doDMLLoad(DataLoadContext context, List<EventData> eventDatas) {
        List<List<EventData>> batchDatas = new ArrayList<List<EventData>>();
        batchDatas.add(eventDatas);
        //处理前,是需要进行字段过滤
        super.doBefore(eventDatas, context, new LoadDataFilter<EventData>() {
            @Override
            public void doFilter(EventData data) {
            }
        });

        if (context.getPipeline().getParameters().isDryRun()) {
            doDryRun(context, batchDatas, true);
        } else {
            doTwoPhase(context, batchDatas, true);
        }
    }

    private void ddlForHdfs(DataLoadContext context, EventData data) {
        //判断是否为truncate操作
        if (!data.getEventType().isTruncate()) {
            return ;
        }

        String path = HdfsLoadDataUtil.getDataLoadPath(data);
        String dataContent = HdfsLoadDataUtil.getTruncateData(data.getTableName());
        final HdfsDataSource dataSource = hdfsDataSourceService.getDataSource(context.getPipeline().getId(),
                context.getDataMediaSource());
        try {
            HDFSUtils.append(dataSource.getFileSystem(), path, dataContent.getBytes());
            context.getProcessedDatas().add(data); // 记录为成功处理的sql
        } catch (Exception e) {
            context.getFailedDatas().add(data);

            Boolean skipDdlException = context.getPipeline().getParameters().getSkipDdlException();
            if (skipDdlException) {
                // do skip
                logger.warn("skip exception for ddl : {} , caused by {}", data, ExceptionUtils.getFullStackTrace(e));
            } else {
                throw new LoadException("#ERROR load truncate data failuer.", e);
            }
        }
    }

    /**
     * 首先进行并行执行，出错后转为串行执行
     */
    private void doTwoPhase(final DataLoadContext context, final List<List<EventData>> totalRows, boolean canBatch) {
        // 预处理下数据
        List<Future<Exception>> results = new ArrayList<Future<Exception>>();
        for (final List<EventData> rows : totalRows) {
            if (CollectionUtils.isEmpty(rows)) {
                continue; // 过滤空记录
            }

            results.add(executor.submit(new HdfsLoadWorker(context, rows, canBatch)));
        }

        if (isPartFailed(context, totalRows, results)) {
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
                    HdfsLoadWorker worker = new HdfsLoadWorker(context, Arrays.asList(retryEventData), false);// 强制设置batch为false
                    doCallableSkipException(worker, retryEventData);
                }
            } else {
                // 直接一批进行处理，减少线程调度
                HdfsLoadWorker worker = new HdfsLoadWorker(context, retryEventDatas, false);// 强制设置batch为false
                doCallableThrowException(worker);
            }

            // 清理failed data数据
            for (EventData data : retryEventDatas) {
                interceptor.after(context, data);// 通知加载完成
            }
        }

    }

    class HdfsLoadWorker extends AbstractLoadWorker {

        private DataLoadContext context;
        private List<EventData> datas;
        private boolean canBatch;

        public HdfsLoadWorker(DataLoadContext context, List<EventData> datas, boolean canBatch) {
            this.context = context;
            this.datas = datas;
            this.canBatch = canBatch;
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
        public Map<ExecuteResult, LoadException> doCustomCall(List<EventData> splitDatas) {
            Map<ExecuteResult, LoadException> result = new HashMap<ExecuteResult, LoadException>();

            final HdfsDataSource dataSource = hdfsDataSourceService.getDataSource(context.getPipeline().getId(),
                    context.getDataMediaSource());

            String path;
            String loadData;
            if (useBatch && canBatch) {
                final EventData data = splitDatas.get(0);// 取第一条即可
                path = HdfsLoadDataUtil.getDataLoadPath(data);
                loadData = HdfsLoadDataUtil.batchprepareDMLLoadData(datas);

                // 更新统计信息
                for (EventData edata : splitDatas) {
                    processStat(context, edata, 1, true);
                }

            } else {
                final EventData data = splitDatas.get(0);// 直接取第一条
                path = HdfsLoadDataUtil.getDataLoadPath(data);
                loadData = HdfsLoadDataUtil.prepareDMLLoadData(data);

                processStat(context, data, 1, false);
            }

            try {
                HDFSUtils.append(dataSource.getFileSystem(), path, loadData.getBytes());
                result.put(ExecuteResult.SUCCESS, null);
            } catch (IOException e) {
                result.put(ExecuteResult.RETRY, new LoadException(ExceptionUtils.getFullStackTrace(e),
                        DbLoadDumper.dumpEventDatas(splitDatas)));
            } catch (Exception e) {
                result.put(ExecuteResult.ERROR, new LoadException(ExceptionUtils.getFullStackTrace(e),
                        DbLoadDumper.dumpEventDatas(splitDatas)));
            }

            return result;
        }

        @Override
        public void processStat(EventData data, DataLoadContext context) {
            HdfsLoadAction.this.processStat(data, context);
        }
    }

    public void setHdfsDataSourceService(DataSourceService hdfsDataSourceService) {
        this.hdfsDataSourceService = hdfsDataSourceService;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public void setRetryWait(int retryWait) {
        this.retryWait = retryWait;
    }
}
