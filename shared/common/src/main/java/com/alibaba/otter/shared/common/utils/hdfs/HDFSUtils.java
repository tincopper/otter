package com.alibaba.otter.shared.common.utils.hdfs;

import com.alibaba.otter.shared.common.utils.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * HDFS 操作工具类
 *
 * @author tangzhongyuan 2018-09-10 18:15
 *
 **/
public class HDFSUtils {

    public static void create(String uri, String username, String path, byte[] bytes, boolean overwrite) throws Exception {
        Assert.assertNotNull(uri);
        Assert.assertNotNull(path);

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(uri), conf, username);

        create(fileSystem, path, bytes, overwrite);
    }

    public static void create(FileSystem fileSystem, String path) throws IOException {
        create(fileSystem, path, null, true);
    }

    public static void create(FileSystem fileSystem, String path, byte[] bytes) throws IOException {
        create(fileSystem, path, bytes, true);
    }

    public static void create(FileSystem fileSystem, String path, byte[] bytes, boolean overwrite) throws IOException {
        Assert.assertNotNull(fileSystem);
        Assert.assertNotNull(path);

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(path), overwrite);
        if (bytes != null && bytes.length > 0) {
            writeData(fsDataOutputStream, bytes);
        }
    }

    public static void append(FileSystem fileSystem, String path, byte[] bytes) throws IOException {
        Assert.assertNotNull(fileSystem);
        Assert.assertNotNull(path);

        Path p = new Path(path);
        if (fileSystem.exists(p)) {
            FSDataOutputStream append = fileSystem.append(p);
            writeData(append, bytes);
        } else {
            create(fileSystem, path, bytes);
        }
    }

    private static void writeData(OutputStream outputStream, byte[] bytes) throws IOException {

        InputStream in = new ByteArrayInputStream(bytes);
        IOUtils.copyBytes(in, outputStream, 4096, true);
    }
}
