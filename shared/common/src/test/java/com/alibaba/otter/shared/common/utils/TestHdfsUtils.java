package com.alibaba.otter.shared.common.utils;

import com.alibaba.otter.shared.common.utils.hdfs.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author tangzhongyuan
 * @create 2018-09-11 14:25
 **/
public class TestHdfsUtils {

    public static final String HDFS_PATH = "hdfs://nn2.hadoop:9000";

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        System.out.println("----------------");
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), conf, "hadoop");
        HDFSUtils.create(fileSystem, "/home/hbase/hadoop/data/tmp/tangzy/test1.txt");
    }
}
