package com.mym.mr.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCount {

    /**mapper,两对KV含义分别是：行偏移量-一行数据，用户标识（手机号），流量bean*/
    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] fields = s.split("\t");                //拆分
            String phoneNo = fields[1];                             //取出手机号
            long upFlow = Long.parseLong(fields[fields.length-3]);  //取出上行流量下行流量
            long dFlow = Long.parseLong(fields[fields.length-2]);

            context.write(new Text(phoneNo), new FlowBean(upFlow, dFlow));
        }
    }

    /**reducer,两对KV含义都是：用户标识（手机号），流量bean*/
    static class FlowCountReducer extends Reducer<Text, FlowBean,Text, FlowBean>{

        ////<183323,bean1><183323,bean2><183323,bean3><183323,bean4>.......
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_dFlow = 0;

            for(FlowBean flowBean:values){
                sum_upFlow += flowBean.getUpFlow();
                sum_dFlow += flowBean.getdFlow();
            }

            context.write(key, new FlowBean(sum_upFlow, sum_dFlow));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        /**指定本程序的jar包所在的本地路径*/
        job.setJarByClass(FlowCount.class);

        /**指定本程序job需要的mapper/reducer类*/
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        /**指定mapper输出数据的kv类型*/
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        /**指定最终本程序输出的kv类型*/
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        /**指定job的输入原始文件/输出文件所在目录：FileInputFormat也指定了切片方式*/
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /**将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行*/
		/*job.submit();*/
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }

}
