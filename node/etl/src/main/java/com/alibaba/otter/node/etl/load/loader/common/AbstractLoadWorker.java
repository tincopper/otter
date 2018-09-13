package com.alibaba.otter.node.etl.load.loader.common;

import com.alibaba.otter.node.etl.load.exception.LoadException;
import com.alibaba.otter.shared.etl.model.EventData;
import org.springframework.util.CollectionUtils;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * 抽象一个通用的数据load worker
 *
 * @author tangzhongyuan
 * @create 2018-09-13 14:28
 **/
public abstract class AbstractLoadWorker implements Callable<Exception> {

    public enum ExecuteResult {
        SUCCESS, ERROR, RETRY
    }

    private List<EventData> allFailedDatas = new ArrayList<EventData>();
    private List<EventData> allProcesedDatas = new ArrayList<EventData>();
    private List<EventData> processedDatas = new ArrayList<EventData>();
    private List<EventData> failedDatas = new ArrayList<EventData>();

    /**
     * 一种通用的方式进行数据load处理，可以重写亦或者不用此方法，直接自己实现</br>
     * </pre>
     * 这里不进行
     * </pre>
     * @param context
     * @param datas
     * @param useBatch
     * @param canBatch
     * @param batchSize
     * @param retry
     * @param retryWait
     * @return
     */
    public Exception doCall(DataLoadContext context, List<EventData> datas,
            boolean useBatch, boolean canBatch, int batchSize, int retry, int retryWait) {

        RuntimeException error = null;
        int index = 0;// 记录下处理成功的记录下标
        for (; index < datas.size();) {
            // 处理数据切分
            final List<EventData> splitDatas = new ArrayList<EventData>();
            if (useBatch && canBatch) {
                int end = (index + batchSize > datas.size()) ? datas.size() : (index + batchSize);
                splitDatas.addAll(datas.subList(index, end));
                index = end;// 移动到下一批次
            } else {
                splitDatas.add(datas.get(index));
                index = index + 1;// 移动到下一条
            }

            int retryCount = 0;
            while (true) {

                if (CollectionUtils.isEmpty(failedDatas) == false) {
                    splitDatas.clear();
                    splitDatas.addAll(failedDatas); // 下次重试时，只处理错误的记录
                } else {
                    failedDatas.addAll(splitDatas); // 先添加为出错记录，可能获取lob,datasource会出错
                }

                failedDatas.clear(); // 先清理
                processedDatas.clear();

                Map<ExecuteResult, LoadException> customCall = doCustomCall(splitDatas);

                if (customCall == null) {
                    break;
                }

                if (customCall.containsKey(ExecuteResult.SUCCESS)) {
                    allFailedDatas.addAll(failedDatas);// 记录一下异常到all记录中
                    allProcesedDatas.addAll(processedDatas);
                    failedDatas.clear();// 清空上一轮的处理
                    processedDatas.clear();
                    break; // do next eventData
                } else if (customCall.containsKey(ExecuteResult.RETRY)) {
                    retryCount = retryCount + 1;// 计数一次
                    // 出现异常，理论上当前的批次都会失败
                    processedDatas.clear();
                    failedDatas.clear();
                    failedDatas.addAll(splitDatas);
                    if (retryCount >= retry) {
                        processFailedDatas(context, datas, index);// 局部处理出错了
                        throw new LoadException(String.format("execute [%s] retry %s times failed",
                                context.getIdentity().toString(),
                                retryCount), customCall.get(ExecuteResult.RETRY));
                    } else {
                        try {
                            int wait = retryCount * retryWait;
                            wait = (wait < retryWait) ? retryWait : wait;
                            Thread.sleep(wait);
                        } catch (InterruptedException ex) {
                            Thread.interrupted();
                            processFailedDatas(context, datas, index);// 局部处理出错了
                            throw new LoadException(ex);
                        }
                    }
                } else {
                    // 出现异常，理论上当前的批次都会失败
                    processedDatas.clear();
                    failedDatas.clear();
                    failedDatas.addAll(splitDatas);
                    processFailedDatas(context, datas, index);// 局部处理出错了
                    throw customCall.get(ExecuteResult.ERROR);
                }
            }
        }

        // 记录一下当前处理过程中失败的记录,affect = 0的记录
        context.getFailedDatas().addAll(allFailedDatas);
        context.getProcessedDatas().addAll(allProcesedDatas);
        return null;
    }

    /**
     * 用于不同的目标源进行自定义的相关数据load处理
     * @param splitDatas
     * @return
     */
    public Map<ExecuteResult, LoadException> doCustomCall(List<EventData> splitDatas) {
        return null;
    }

    /**
     * 进行统计跟踪处理
     * @param data
     * @param context
     */
    public abstract void processStat(EventData data, DataLoadContext context);

    // 出现异常回滚了，记录一下异常记录
    private void processFailedDatas(DataLoadContext context, List<EventData> datas, int index) {
        allFailedDatas.addAll(failedDatas);// 添加失败记录
        context.getFailedDatas().addAll(allFailedDatas);// 添加历史出错记录
        for (; index < datas.size(); index++) { // 记录一下未处理的数据
            context.getFailedDatas().add(datas.get(index));
        }
        // 这里不需要添加当前成功记录，出现异常后会rollback所有的成功记录，比如processDatas有记录，但在commit出现失败
        // (bugfix)
        allProcesedDatas.addAll(processedDatas);
        context.getProcessedDatas().addAll(allProcesedDatas);// 添加历史成功记录
    }

    public void processStat(DataLoadContext context, EventData data, int affect, boolean batch) {
        if (batch && (affect < 1 && affect != Statement.SUCCESS_NO_INFO)) {
            failedDatas.add(data); // 记录到错误的临时队列，进行重试处理
        } else if (!batch && affect < 1) {
            failedDatas.add(data);// 记录到错误的临时队列，进行重试处理
        } else {
            processedDatas.add(data); // 记录到成功的临时队列，commit也可能会失败。所以这记录也可能需要进行重试
            processStat(data, context);
        }
    }
}
