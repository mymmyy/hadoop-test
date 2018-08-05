package com.mym.mr.mrjoin;

import com.mym.mr.worldcountcombiner.WorldCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class MRJoin {

    static class RJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {

        private InfoBean bean = new InfoBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineStr = value.toString();

            //通过文件名判断是哪个类型的数据
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            String[] fields = lineStr.split("\t");

            String pid = "";
            if(fileName.startsWith("order")){
                pid = fields[2];
                bean.set(Integer.parseInt(fields[0]), fields[1], pid, Integer.parseInt(fields[3]), "", 0, 0, "0");
            }else{
                pid = fields[0];
                bean.set(0, "", pid, 0, fields[1], Integer.parseInt(fields[2]), Float.parseFloat(fields[3]), "1");
            }

            context.write(new Text(pid), bean);
        }
    }

    static class RJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {

            InfoBean pbBean = null;
            ArrayList<InfoBean> beans = new ArrayList<>();

            for(InfoBean bean:values){

                if("1".equals(bean.getFlag())){
                    pbBean = bean;
                }else{
                    beans.add(bean);
                }
            }

            //合并
            for(InfoBean b:beans){
                if(pbBean != null){
                    b.setPname(pbBean.getPname());
                    b.setCategory_id(pbBean.getCategory_id());
                    b.setPrice(pbBean.getPrice());

                    context.write(b, NullWritable.get());
                }
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
                /*定义conf*/
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        /*设置本程序类*/
        job.setJarByClass(WorldCount.class);

        job.setJarByClass(MRJoin.class);
        // 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReducer.class);

        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        // 指定最终输出的数据的kv类型
        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/* job.submit(); */
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}
