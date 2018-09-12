package com.alibaba.otter.node.etl.load.loader.db.context;

import com.alibaba.otter.node.etl.load.loader.AbstractLoadContext;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.etl.model.EventData;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author tangzhongyuan
 * @create 2018-09-10 16:10
 **/
public class HdfsLoadContext extends AbstractLoadContext<EventData> {

    private static final long serialVersionUID = -847066316546635572L;

    private List<EventData> lastProcessedDatas;                      // 上一轮的已录入的记录，可能会有多次失败需要合并多次已录入的数据
    private DataMediaSource dataMediaSource;

    public HdfsLoadContext() {
        lastProcessedDatas = Collections.synchronizedList(new LinkedList<EventData>());
        prepareDatas = Collections.synchronizedList(new LinkedList<EventData>());
        processedDatas = Collections.synchronizedList(new LinkedList<EventData>());
        failedDatas = Collections.synchronizedList(new LinkedList<EventData>());
    }

    public List<EventData> getLastProcessedDatas() {
        return lastProcessedDatas;
    }

    public void setLastProcessedDatas(List<EventData> lastProcessedDatas) {
        this.lastProcessedDatas = lastProcessedDatas;
    }

    public DataMediaSource getDataMediaSource() {
        return dataMediaSource;
    }

    public void setDataMediaSource(DataMediaSource dataMediaSource) {
        this.dataMediaSource = dataMediaSource;
    }
}
