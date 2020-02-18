package com.zhangjie.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;


public class HDFSClient {

    public static void main(String[] args) throws IOException, InterruptedException {

        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://node2:9000");
        // 获取hdfs客户端对象
//        FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"),conf,"root");
        // 在hdfs上创建文件夹
        fs.mkdirs(new Path("/zhangjie/test"));
        // 关闭资源
        fs.close();

    }

    /**
     * 1 文件上传：本地文件上传到HDFS系统指定的目录中
     * 参数优先级：1.客户端代码中设置的值 > 2.ClassPath下用户自定义的配置文件 > 3.服务器的默认配置
     */
    @Test
    public void testCopyFromLocal() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 1 获取fs对象
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"),conf,"root");

        // 2 开始上传
        fs.copyFromLocalFile(new Path("F:/装机软件/大数据相关/hadoop学习相关/copyFromLocal.txt"),new Path("/zhangjie/test"));

        // 3 关闭资源
        fs.close();
    }

    /**
     * 2.文件下载：文件从HDFS文件系统下载到本地
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testCopyToLocal() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 1 获取fs对象
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"),conf,"root");
        // 2 执行下载操作
        // boolean delSrc 是否删除源文件
        // Path src 要下载的文件在HDFS中的路径
        // Path dst 将HDFS中的文件下载到哪里
        // boolean useRawLocalFileSystem 是否开启文件校验

        fs.copyToLocalFile(false,new Path("/zhangjie/test/copyFromLocal.txt")
                ,new Path("F:/装机软件/大数据相关/hadoop学习相关/copyFromLocal1.txt")
        ,false);

        // 3 关闭资源
        fs.close();
    }

    /**
     * 3 文件的删除
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testDelete() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 1 获取fs对象
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"),conf,"root");
        // 2 执行删除文件的操作
        fs.delete(new Path("/zhangjie/test/copyFromLocal.txt"),false);

        // 3 关闭资源
        fs.close();
    }

    /**
     * 4 文件的重命名
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testRename() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 1 获取fs对象
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"),conf,"root");
        // 2 执行删除文件的操作
        fs.rename(new Path("/zhangjie/test/copyFromLocal.txt"),new Path("/zhangjie/test/rename.txt"));

        // 3 关闭资源
        fs.close();
    }

    /**
     * 5 文件详情查看
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testListFiles() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 1 获取fs对象
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"),conf,"root");
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(new Path("/"), true);
        while (fileList.hasNext()){
//             length a file's length
//             isdir if the path is a directory
//             block_replication the file's replication factor
//             blocksize a file's block size
//             modification_time a file's modification time
//             access_time a file's access time
//             permission a file's permission
//             owner a file's owner
//             group a file's group
//             symlink symlink if the path is a symbolic link
//             path the path's qualified name
//             locations a file's block locations
            LocatedFileStatus fileStatus = fileList.next();
            System.out.println(fileStatus.getPath().getName()+":"+fileStatus.getLen() + "<------>" + (fileStatus.isDirectory()?fileStatus.isDirectory():"not a directory") + "<------>" +
                    fileStatus.getReplication() + "<------>" +
                    fileStatus.getBlockSize() + "<------>" + fileStatus.getModificationTime() + "<------>" + fileStatus.getAccessTime() + "<------>" +
                    fileStatus.getPermission()+"<------>"+ fileStatus.getOwner() + "<------>" + fileStatus.getGroup() + "<------>" +(
                    fileStatus.isSymlink()?fileStatus.getSymlink():"not symlink" )+ "<------>" + fileStatus.getPath() + "<------>" + Arrays.toString(fileStatus.getBlockLocations()));
            System.out.println("-----------------------华丽的分割线-----------------------");
        }

        // 3 关闭资源
        fs.close();
    }

    /**
     * 6 判断是否为文件夹或者文件
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testIsDirectory() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 1 获取fs对象
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"),conf,"root");
        // 2 获取文件状态
        FileStatus[] fileStatuses =fs.listStatus(new Path("/"));
        for (FileStatus fileStatus: fileStatuses) {
            if(fileStatus.isFile()){
                // 文件
                System.out.println("f:"+fileStatus.getPath().getName()+",size:"+fileStatus.getLen()+" B");
            }else if (fileStatus.isDirectory()){
                // 文件夹
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }
}
