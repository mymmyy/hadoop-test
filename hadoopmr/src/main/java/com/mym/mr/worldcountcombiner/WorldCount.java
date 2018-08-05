package com.mym.mr.worldcountcombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WorldCount {

    /**mapper*/
    static class CombinerTestMapper extends Mapper<LongWritable,Text, Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineStr = value.toString();
            String[] worlds = lineStr.split(" ");
            for(String sl:worlds){
                context.write(new Text(sl),new IntWritable(1));
            }

        }
    }

    /**reducer*/
    static class CombinerTestReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int worldCount = 0;
            for(IntWritable i:values){
                worldCount += i.get();
            }

            context.write(key, new IntWritable(worldCount));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*定义conf*/
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        /*设置本程序类*/
        job.setJarByClass(WorldCount.class);

        /*设置mapper/reducer*/
        job.setMapperClass(CombinerTestMapper.class);
        job.setReducerClass(CombinerTestReducer.class);

        /*指定mapper输出数据的KV数据类型*/
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /*指定最终输出数据的KV类型*/
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /**设置combiner，不能影响原数据，可以直接使用reducer*/
        job.setCombinerClass(CombinerTestReducer.class);

        /*指定job的输入原始文件目录：job处理的数据源*/
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        /*指定job的输出结果所在目录*/
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /*提交任务*/
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }


}
