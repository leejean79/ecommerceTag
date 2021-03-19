package com.leejean.tag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TagPartitioner extends Partitioner<Text, IntWritable> {
// 根据combokey中的id进行分区
//    public int getPartition(ComboKey comboKey, IntWritable intWritable, int numPartitions) {
//        String  str = comboKey.getId();
//        int id = Integer.parseInt(str);
//        return id % numPartitions;
//    }

    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        //根据id进行分区
        String k = text.toString();
        String[] ks = k.split(",");
        int id = Integer.parseInt(ks[0]);
        return id % numPartitions;
    }
}
