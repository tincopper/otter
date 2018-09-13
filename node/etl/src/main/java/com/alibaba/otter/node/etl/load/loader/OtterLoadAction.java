package com.alibaba.otter.node.etl.load.loader;

import com.alibaba.otter.node.etl.load.loader.common.DataLoadContext;
import com.alibaba.otter.node.etl.load.loader.weight.WeightController;
import com.alibaba.otter.shared.etl.model.RowBatch;

public interface OtterLoadAction {

    DataLoadContext load(RowBatch rowBatch, WeightController controller);
}
