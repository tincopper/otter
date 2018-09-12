package com.alibaba.otter.node.etl.common.datasource.impl;

import com.alibaba.otter.node.etl.common.datasource.DataSourceService;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.common.model.config.data.DataMediaType;
import com.alibaba.otter.shared.common.model.config.data.db.DbMediaSource;
import com.alibaba.otter.shared.common.model.config.data.hdfs.HdfsDataSource;
import com.google.common.base.Function;
import com.google.common.collect.OtterMigrateMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.Map;

/**
 * HDFS data source operate
 * @author tangzhongyuan
 * @create 2018-09-10 10:02
 **/
public class HDFSDataSourceService implements DataSourceService, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(HDFSDataSourceService.class);

    //用于扩展
    //private List<DataSourceHanlder> dataSourceHandlers;
    private Map<Long, Map<DbMediaSource, HdfsDataSource>> dataSources;

    public HDFSDataSourceService() {
        // 构建第一层map
        dataSources = OtterMigrateMap.makeComputingMap(new Function<Long, Map<DbMediaSource, HdfsDataSource>>() {

            public Map<DbMediaSource, HdfsDataSource> apply(final Long pipelineId) {
                // 构建第二层map
                return OtterMigrateMap.makeComputingMap(new Function<DbMediaSource, HdfsDataSource>() {

                    public HdfsDataSource apply(DbMediaSource hdfsMediaSource) {

                        // 扩展功能,可以自定义一些自己实现的 dataSource
                        /*DataSource customDataSource = preCreate(pipelineId, hdfsMediaSource);
                        if (customDataSource != null) {
                            return customDataSource;
                        }*/

                        return createDataSource(hdfsMediaSource.getUrl(),
                                hdfsMediaSource.getUsername(), hdfsMediaSource.getType());
                    }

                });
            }
        });

    }

    private HdfsDataSource createDataSource(String url, String userName, DataMediaType dataMediaType) {
        HdfsDataSource dataSource = new HdfsDataSource();
        dataSource.setUrl(url);
        dataSource.setUsername(userName);

        return dataSource;
    }

    @Override
    public HdfsDataSource getDataSource(long pipelineId, DataMediaSource dataMediaSource) {
        Assert.notNull(dataMediaSource);
        return dataSources.get(pipelineId).get(dataMediaSource);
    }

    @Override
    public void destroy(Long pipelineId) {
        Map<DbMediaSource, HdfsDataSource> sources = dataSources.remove(pipelineId);
        if (sources != null) {
            for (HdfsDataSource source : sources.values()) {
                try {
                    // for filter to destroy custom datasource
                    //if (letHandlerDestroyIfSupport(pipelineId, fileSystem)) {
                    //    continue;
                    //}

                    // fallback for regular destroy
                    source.close();
                } catch (IOException e) {
                    logger.error("ERROR ## close the fileSystem has an error", e);
                }
            }

            sources.clear();
        }
    }

    @Override
    public void destroy() {
        for (Long pipelineId : dataSources.keySet()) {
            destroy(pipelineId);
        }
    }
}
