package com.mym.mr.worldcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 相当于一个yarn集群的客户端，或者说一个mapreducer任务的启动类
 * 需要在此封装我们的mr程序的相关运行参数，指定jar包
 * 最后提交给yarn
 * @author
 *
 */
public class WorldCountDriver {

    final static String resourceManagerAddr = "192.168.31.201:9090";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args == null || args.length == 0){
            args = new String[2];
            args[0] = "hdfs://"+resourceManagerAddr+"/wordcount/input/wordcount.txt";
            args[1] = "hdfs://"+resourceManagerAddr+"/wordcount/output";
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WorldCountDriver.class);

        /*指定本业务job要使用的mapper/Reducer业务类*/
        job.setMapperClass(WorldCountMapper.class);
        job.setReducerClass(WorldCountReducer.class);

        /*指定mapper输出数据的KV数据类型*/
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /*指定最终输出数据的KV类型*/
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /*指定job的输入原始文件目录：job处理的数据源*/
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        /*指定job的输出结果所在目录*/
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

}
