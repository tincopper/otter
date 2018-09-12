package com.alibaba.otter.shared.common.model.config.data.hdfs;

import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;

/**
 * @author tangzhongyuan
 * @create 2018-09-10 15:13
 **/
public class HdfsMediaSource extends DataMediaSource {

    private static final long serialVersionUID = 2367497311183595805L;

    private String            url;
    private String            username;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
