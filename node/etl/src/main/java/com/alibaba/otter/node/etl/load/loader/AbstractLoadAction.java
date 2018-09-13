package com.alibaba.otter.node.etl.load.loader;

import com.alibaba.otter.node.common.config.ConfigClientService;
import com.alibaba.otter.node.etl.load.exception.LoadException;
import com.alibaba.otter.node.etl.load.loader.common.DataLoadContext;
import com.alibaba.otter.node.etl.load.loader.common.LoadDataFilter;
import com.alibaba.otter.node.etl.load.loader.db.DbLoadAction;
import com.alibaba.otter.node.etl.load.loader.db.DbLoadDumper;
import com.alibaba.otter.node.etl.load.loader.interceptor.LoadInterceptor;
import com.alibaba.otter.node.etl.load.loader.weight.WeightBuckets;
import com.alibaba.otter.node.etl.load.loader.weight.WeightController;
import com.alibaba.otter.shared.common.model.config.ConfigHelper;
import com.alibaba.otter.shared.common.model.config.channel.Channel;
import com.alibaba.otter.shared.common.model.config.data.DataMediaPair;
import com.alibaba.otter.shared.common.model.config.pipeline.Pipeline;
import com.alibaba.otter.shared.common.utils.thread.NamedThreadFactory;
import com.alibaba.otter.shared.etl.model.EventData;
import com.alibaba.otter.shared.etl.model.EventType;
import com.alibaba.otter.shared.etl.model.Identity;
import com.alibaba.otter.shared.etl.model.RowBatch;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.*;

/**
 * @author tangzhongyuan
 * @create 2018-09-12 10:52
 **/
public abstract class AbstractLoadAction implements OtterLoadAction, InitializingBean, DisposableBean {

    private static final Logger logger             = LoggerFactory.getLogger(DbLoadAction.class);
    private static final String WORKER_NAME        = "DataLoadAction";
    private static final int    DEFAULT_POOL_SIZE  = 5;
    private int                 poolSize           = DEFAULT_POOL_SIZE;
    public  LoadInterceptor     interceptor;
    public  ExecutorService     executor;
    public  ConfigClientService configClientService;
    public  LoadStatsTracker    loadStatsTracker;
    public  boolean             useBatch;

    /**
     * 返回结果为已处理成功的记录
     */
    @Override
    public DataLoadContext load(RowBatch rowBatch, WeightController controller) {
        Assert.notNull(rowBatch);
        Identity identity = rowBatch.getIdentity();
        DataLoadContext context = this.buildContext(identity);

        try {
            List<EventData> datas = rowBatch.getDatas();
            context.setPrepareDatas(datas);
            // 执行重复录入数据过滤
            datas = context.getPrepareDatas();
            if (datas == null || datas.size() == 0) {
                logger.info("##no eventdata for load, return");
                return context;
            }

            // 因为所有的数据在DbBatchLoader已按照DateMediaSource进行归好类，不同数据源介质会有不同的DbLoadAction进行处理
            // 设置media source时，只需要取第一节点的source即可
            context.setDataMediaSource(ConfigHelper.findDataMedia(context.getPipeline(), datas.get(0).getTableId())
                    .getSource());
            interceptor.prepare(context);
            // 执行重复录入数据过滤
            datas = context.getPrepareDatas();
            // 处理下ddl语句，ddl/dml语句不可能是在同一个batch中，由canal进行控制
            // 主要考虑ddl的幂等性问题，尽可能一个ddl一个batch，失败或者回滚都只针对这条sql
            if (isDdlDatas(datas)) {
                doDdl(context, datas);
            } else {
                WeightBuckets<EventData> buckets = buildWeightBuckets(context, datas);
                List<Long> weights = buckets.weights();
                controller.start(weights);// weights可能为空，也得调用start方法
                if (CollectionUtils.isEmpty(datas)) {
                    logger.info("##no eventdata for load");
                }
                adjustPoolSize(context); // 根据manager配置调整线程池
                adjustConfig(context); // 调整一下运行参数
                // 按权重构建数据对象
                // 处理数据
                for (int i = 0; i < weights.size(); i++) {
                    Long weight = weights.get(i);
                    controller.await(weight.intValue());
                    // 处理同一个weight下的数据
                    List<EventData> items = buckets.getItems(weight);
                    logger.debug("##start load for weight:" + weight);

                    // 预处理下数据
                    doDMLLoad(context, items);

                    controller.single(weight.intValue());
                    logger.debug("##end load for weight:" + weight);
                }
            }
            interceptor.commit(context);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            interceptor.error(context);
        } catch (Exception e) {
            interceptor.error(context);
            throw new LoadException(e);
        }

        return context;// 返回处理成功的记录
    }

    public DataLoadContext buildContext(Identity identity) {
        DataLoadContext context = new DataLoadContext();
        context.setIdentity(identity);
        Channel channel = configClientService.findChannel(identity.getChannelId());
        Pipeline pipeline = configClientService.findPipeline(identity.getPipelineId());
        context.setChannel(channel);
        context.setPipeline(pipeline);
        return context;
    }

    /**
     * 分析整个数据，将datas划分为多个批次. ddl sql前的DML并发执行，然后串行执行ddl后，再并发执行DML
     *
     * @return
     */
    public boolean isDdlDatas(List<EventData> eventDatas) {
        boolean result = false;
        for (EventData eventData : eventDatas) {
            result |= eventData.getEventType().isDdl();
            if (result && !eventData.getEventType().isDdl()) {
                throw new LoadException("ddl/dml can't be in one batch, it's may be a bug , pls submit issues.",
                        DbLoadDumper.dumpEventDatas(eventDatas));
            }
        }

        return result;
    }

    /**
     * 构建基于weight权重分组的item集合列表
     */
    public WeightBuckets<EventData> buildWeightBuckets(DataLoadContext context, List<EventData> datas) {
        WeightBuckets<EventData> buckets = new WeightBuckets<EventData>();
        for (EventData data : datas) {
            // 获取对应的weight
            DataMediaPair pair = ConfigHelper.findDataMediaPair(context.getPipeline(), data.getPairId());
            buckets.addItem(pair.getPushWeight(), data);
        }

        return buckets;
    }

    /**
     * 执行ddl的调用，处理逻辑比较简单: 串行调用
     *
     * @param context
     * @param eventDatas
     */
    protected abstract void doDdl(DataLoadContext context, List<EventData> eventDatas);

    protected abstract void doDMLLoad(final DataLoadContext context, List<EventData> eventDatas);

    protected void doBefore(List<EventData> items, final DataLoadContext context, final LoadDataFilter<EventData> loadData) {
        for (final EventData item : items) {
            boolean filter = interceptor.before(context, item);
            if (!filter) {
                loadData.doFilter(item);// 进行分类
            }
        }
    }

    public void doDryRun(DataLoadContext context, List<List<EventData>> totalRows, boolean canBatch) {
        for (List<EventData> rows : totalRows) {
            if (CollectionUtils.isEmpty(rows)) {
                continue; // 过滤空记录
            }

            for (EventData row : rows) {
                processStat(row, context);// 直接记录成功状态
            }

            context.getProcessedDatas().addAll(rows);
        }
    }

    // 调整一下线程池
    public void adjustPoolSize(DataLoadContext context) {
        Pipeline pipeline = context.getPipeline();
        int newPoolSize = pipeline.getParameters().getLoadPoolSize();
        if (newPoolSize != poolSize) {
            poolSize = newPoolSize;
            if (executor instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
                pool.setCorePoolSize(newPoolSize);
                pool.setMaximumPoolSize(newPoolSize);
            }
        }
    }

    public void adjustConfig(DataLoadContext context) {
        Pipeline pipeline = context.getPipeline();
        this.useBatch = pipeline.getParameters().isUseBatch();
    }

    public void afterPropertiesSet() throws Exception {
        executor = new ThreadPoolExecutor(poolSize,
                poolSize,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue(poolSize * 4),
                new NamedThreadFactory(WORKER_NAME),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void destroy() throws Exception {
        executor.shutdownNow();
    }

    public void processStat(EventData data, DataLoadContext context) {
        LoadStatsTracker.LoadThroughput throughput = loadStatsTracker.getStat(context.getIdentity());
        LoadStatsTracker.LoadCounter counter = throughput.getStat(data.getPairId());
        EventType type = data.getEventType();
        if (type.isInsert()) {
            counter.getInsertCount().incrementAndGet();
        } else if (type.isUpdate()) {
            counter.getUpdateCount().incrementAndGet();
        } else if (type.isDelete()) {
            counter.getDeleteCount().incrementAndGet();
        }

        counter.getRowCount().incrementAndGet();
        counter.getRowSize().addAndGet(calculateSize(data));
    }

    // 大致估算一下row记录的大小
    public long calculateSize(EventData data) {
        // long size = 0L;
        // size += data.getKeys().toString().getBytes().length - 12 -
        // data.getKeys().size() + 1L;
        // size += data.getColumns().toString().getBytes().length - 12 -
        // data.getKeys().size() + 1L;
        // return size;

        // byte[] bytes = JsonUtils.marshalToByte(data);// 走序列化的方式快速计算一下大小
        // return bytes.length;

        return data.getSize();// 数据不做计算，避免影响性能
    }

    public boolean isPartFailed(DataLoadContext context, List<List<EventData>> totalRows, List<Future<Exception>> results) {
        for (int i = 0; i < results.size(); i++) {
            Future<Exception> result = results.get(i);
            Exception ex = null;
            try {
                ex = result.get();
                for (EventData data : totalRows.get(i)) {
                    interceptor.after(context, data);// 通知加载完成
                }
            } catch (Exception e) {
                ex = e;
            }

            if (ex != null) {
                logger.warn("##load phase one failed!", ex);
                return true;
            }
        }
        return false;
    }

    public void doCallableSkipException(Callable<Exception> worker, EventData retryEventData) {
        try {
            Exception ex = worker.call();
            if (ex != null) {
                // do skip
                logger.warn("skip exception for data : {} , caused by {}",
                        retryEventData,
                        ExceptionUtils.getFullStackTrace(ex));
            }
        } catch (Exception ex) {
            // do skip
            logger.warn("skip exception for data : {} , caused by {}",
                    retryEventData,
                    ExceptionUtils.getFullStackTrace(ex));
        }
    }

    public void doCallableThrowException(Callable<Exception> worker) {
        try {
            Exception ex = worker.call();
            if (ex != null) {
                throw ex; // 自己抛自己接
            }
        } catch (Exception ex) {
            logger.error("##load phase two failed!", ex);
            throw new LoadException(ex);
        }
    }

    // =============== setter / getter ===============

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public void setInterceptor(LoadInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public void setConfigClientService(ConfigClientService configClientService) {
        this.configClientService = configClientService;
    }

    public void setLoadStatsTracker(LoadStatsTracker loadStatsTracker) {
        this.loadStatsTracker = loadStatsTracker;
    }

}
