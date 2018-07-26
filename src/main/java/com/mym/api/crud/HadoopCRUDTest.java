package com.mym.api.crud;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopCRUDTest {

    FileSystem fs = null;

    @Before
    public void connect() throws IOException, URISyntaxException, InterruptedException {

        //获取客户端实例
        // 构造一个配置参数对象，设置一个参数：我们要访问的hdfs的URI
        // 从而FileSystem.get()方法就知道应该是去构造一个访问hdfs文件系统的客户端，以及hdfs的访问地址
        // new Configuration();的时候，它就会去加载jar包中的hdfs-default.xml
        // 然后再加载classpath下的hdfs-site.xml
        Configuration conf = new Configuration();

        /**
         * 参数优先级： 1、客户端代码中设置的值 2、classpath下的用户自定义配置文件 3、然后是服务器的默认配置
         */
        conf.set("fs.defaultFS","hdfs://192.168.31.201:9000");
        conf.set("dfs.replication","3");
        fs = FileSystem.get(conf);


        // 如果这样去获取，那conf里面就可以不要配"fs.defaultFS"参数，而且，这个客户端的身份标识已经是hadoop用户
        //fs = FileSystem.get(new URI("hdfs://192.168.31.201:9000"), conf, "hadoop");
    }

    /**
     * 上传文件
     * @throws IOException
     */
    @Test
    public void uploadFile() throws IOException {
        //本地文件路径
        Path src = new Path("C:\\Users\\明柯\\Desktop\\证件照.jpg");
        // 要上传到hdfs的目标路径
        Path dst = new Path("/my");
        //执行
        fs.copyFromLocalFile(src, dst);
        System.out.println("success copyFromLocalFile!!");
    }

    /**
     * 下载文件
     */
    @Test
    public void downloadFile() throws IOException {
        //本地接受路径
        Path dst = new Path("C:\\Users\\明柯\\Desktop\\fromHdfs.jpg");
        // hdfs的目标路径
        Path src = new Path("/my");
        //执行
        fs.copyToLocalFile(src, dst);
        System.out.println("success copyToLocalFile!!");
    }

    /**
     * 查看目录信息，只显示文件
     *
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    @Test
    public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {

        // 思考：为什么返回迭代器，而不是List之类的容器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("--------------为angelababy打印的分割线--------------");
        }
    }

    /**
     * 查看文件及文件夹信息
     *
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    @Test
    public void testListAll() throws FileNotFoundException, IllegalArgumentException, IOException {

        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        String flag = "目录：";
        for (FileStatus fstatus : listStatus) {
            if (fstatus.isFile())  flag = "文件：";
            System.out.println(flag + fstatus.getPath().getName());
        }
    }



    @Test
    public void testMkdirAndDeleteAndRename() throws IllegalArgumentException, IOException {

        // 创建目录
        fs.mkdirs(new Path("/a1"));

        // 删除文件夹 ，如果是非空文件夹，参数2必须给值true
        fs.delete(new Path("/aaa"), true);

        // 重命名文件或文件夹
        fs.rename(new Path("/a1"), new Path("/a2"));

    }


    @After
    public void close() throws IOException {
        fs.close();
    }

}
