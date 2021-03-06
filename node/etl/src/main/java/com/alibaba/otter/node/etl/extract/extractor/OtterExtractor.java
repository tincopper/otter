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

package com.alibaba.otter.node.etl.extract.extractor;

import com.alibaba.otter.node.etl.extract.exceptions.ExtractException;
import com.alibaba.otter.shared.etl.model.DbBatch;

/**
 * 组装数据,有多种来源，mysql,oracle,store,file等.
 */
public interface OtterExtractor<P> {

    /**
     * 数据装配
     */
    void extract(P param) throws ExtractException;

    /**
     * 是否跳过
     * @return
     */
    boolean skip(DbBatch dbBatch);
}
