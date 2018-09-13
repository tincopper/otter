package com.alibaba.otter.node.etl.load.loader.common;

import com.alibaba.otter.node.etl.load.loader.AbstractLoadContext;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.etl.model.EventData;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * 通用数据load上下文
 * @author tangzhongyuan 2018-09-12 10:56
 **/
public class DataLoadContext extends AbstractLoadContext<EventData> {

    private static final long serialVersionUID = 8344632191711034846L;

    private DataMediaSource dataMediaSource;

    public DataLoadContext(){
    }

    public DataMediaSource getDataMediaSource() {
        return dataMediaSource;
    }

    public void setDataMediaSource(DataMediaSource dataMediaSource) {
        this.dataMediaSource = dataMediaSource;
    }
}
