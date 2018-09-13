package com.alibaba.otter.node.etl.load.loader.common;

import com.alibaba.otter.node.etl.load.loader.AbstractLoadContext;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * 通用数据load上下文
 * @author tangzhongyuan
 * @create 2018-09-12 10:56
 **/
public class DataLoadContext<T> extends AbstractLoadContext<T> {

    private static final long serialVersionUID = 8344632191711034846L;

    private List<T> lastProcessedDatas;                      // 上一轮的已录入的记录，可能会有多次失败需要合并多次已录入的数据
    private DataMediaSource dataMediaSource;

    public DataLoadContext(){
        lastProcessedDatas = Collections.synchronizedList(new LinkedList<T>());
    }

    public DataMediaSource getDataMediaSource() {
        return dataMediaSource;
    }

    public void setDataMediaSource(DataMediaSource dataMediaSource) {
        this.dataMediaSource = dataMediaSource;
    }
}
