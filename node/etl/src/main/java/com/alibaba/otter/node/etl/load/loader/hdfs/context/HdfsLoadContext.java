package com.alibaba.otter.node.etl.load.loader.hdfs.context;

import com.alibaba.otter.node.etl.load.loader.AbstractLoadContext;
import com.alibaba.otter.node.etl.load.loader.common.DataLoadContext;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.etl.model.EventData;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author tangzhongyuan
 * @create 2018-09-10 16:10
 **/
public class HdfsLoadContext extends DataLoadContext {

    private static final long serialVersionUID = -847066316546635572L;

    public HdfsLoadContext() {
        super();
    }
}
