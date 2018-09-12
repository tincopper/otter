package com.alibaba.otter.shared.common.model.config.data.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

/**
 * @author tangzhongyuan
 * @create 2018-09-11 11:08
 **/
public class HdfsDataSource implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(HdfsDataSource.class);

    private String            url;

    private String            username;

    private Configuration configuration = null;
    private FileSystem fileSystem = null;

    public FileSystem getFileSystem() {
        configuration = new Configuration();
        //configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        //configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            // 第一参数是服务器的URI，第二个参数是配置对象，第三个参数是文件系统的用户名
            fileSystem = FileSystem.get(new URI(url), configuration, username);
        } catch (Exception e) {
            logger.error("ERROR ## get file system has error. [{}]", e.getMessage(), e);
        }
        return fileSystem;
    }

    @Override
    public void close() throws IOException {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }

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
