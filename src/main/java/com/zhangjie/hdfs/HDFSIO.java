package com.zhangjie.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSIO {
    /**
     * 1 将本地文件以IO流的形式拷贝到HDFS系统的指定目录中并创建文件
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 1 获取fs对象
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"),conf,"root");
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("F:\\装机软件\\大数据相关\\hadoop学习相关\\IO1.txt"));
        // 3 获取输出流
        FSDataOutputStream fsdos = fs.create(new Path("/zhangjie/IO/IO1.txt"));
        // 4 流对拷
        IOUtils.copyBytes(fis,fsdos,conf);
        // 5 关闭资源
        IOUtils.closeStream(fsdos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 2 将HDFS的文件以流的形式下载到本地指定的路径重写或者创建文件
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 1 获取fs对象
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"),conf,"root");
        // 2 获取输入流
        FSDataInputStream fsdis = fs.open(new Path("/zhangjie/IO/IO1.txt"));
        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("F:\\装机软件\\大数据相关\\hadoop学习相关\\IO1.txt"));
        // 4 流对拷
        IOUtils.copyBytes(fsdis,fos,conf);
        // 5 关闭资源
        IOUtils.closeStream(fsdis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 3 分块读取HDFS上的大文件
     *   3.1 下载第一块
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void readFileSeek1() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 1 获取fs对象
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"),conf,"root");
        // 2 获取输入流
        FSDataInputStream fsdis = fs.open(new Path("/jdk-8u231-linux-x64.tar.gz"));
        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(
                new File("F:\\装机软件\\大数据相关\\hadoop学习相关\\jdk-8u231-linux-x64.tar.gz.part1")
        );
        // 4 流对拷（只复制128M，一个block的大小）
        byte[] buffer = new byte[1024];
        for (int i = 0; i < 1024 * 128 ; i++) {
            fsdis.read(buffer);
            fos.write(buffer);
        }

        // 5 关闭资源
        IOUtils.closeStream(fsdis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 3 分块读取HDFS上的大文件
     *   3.2 下载第二块
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void readFileSeek2() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 1 获取fs对象
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"),conf,"root");
        // 2 获取输入流
        FSDataInputStream fsdis = fs.open(new Path("/jdk-8u231-linux-x64.tar.gz"));
        // 3 指定读取起点
        fsdis.seek(1024*1024*128);
        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(
                new File("F:\\装机软件\\大数据相关\\hadoop学习相关\\jdk-8u231-linux-x64.tar.gz.part2")
        );
        // 5 流对拷
        IOUtils.copyBytes(fsdis,fos,conf);
        // 6 关闭资源
        IOUtils.closeStream(fsdis);
        IOUtils.closeStream(fos);
        fs.close();
    }
}
