package com.mym.mr.worldcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * KEYIN, VALUEIN 对应  mapper输出的KEYOUT,VALUEOUT类型对应
 *
 * KEYOUT, VALUEOUT 是自定义reduce逻辑处理结果的输出数据类型
 * KEYOUT是单词
 * VLAUEOUT是总次数
 * @author
 *
 */
public class WorldCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    /**
     * 给过来的数据结构如下
     * <angelababy,1><angelababy,1><angelababy,1><angelababy,1><angelababy,1>
     * <hello,1><hello,1><hello,1><hello,1><hello,1><hello,1>
     * <banana,1><banana,1><banana,1><banana,1><banana,1><banana,1>
     * 入参key，是一组相同单词kv对的key
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        //做统计
        for(IntWritable intWritable: values){
            count += intWritable.get();
        }

        //本reducer的输出
        context.write(key, new IntWritable(count));

    }
}
