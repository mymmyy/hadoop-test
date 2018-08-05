package com.mym.mr.flowpartitionercount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * K  V 对应的是map输出kv的类型
 */
public class ProvincePartition extends Partitioner<Text, FlowBean> {

    /**预先分区：是对给到reducer的数据进行分区*/
    public static HashMap<String, Integer> proviceDict = new HashMap<String, Integer>();
    static{
        proviceDict.put("136", 0);
        proviceDict.put("137", 1);
        proviceDict.put("138", 2);
        proviceDict.put("139", 3);
    }
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String prefix = text.toString().substring(0, 3);
        Integer provinceId = proviceDict.get(prefix);
        //返回分区
        return provinceId ==  null ? 4 : provinceId;
    }
}
