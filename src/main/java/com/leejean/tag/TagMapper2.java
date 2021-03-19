package com.leejean.tag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
        输入：
        77287793,音响效果好	26
        77287793,音响效果差	1
        77287793,高大上	16
        输出：
        77287793    “高大上:16”
        .....
 */

public class TagMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Text keyOut = new Text();
        Text valueOut = new Text();

        String line = value.toString();
        //提取key
        String[] fields = line.split(",");
        String k = fields[0];

        //整合value
        //将“音响效果差	1”替换为“音响效果差:1”
        String v = fields[1].replaceAll("\t", ":");
        keyOut.set(k);
        valueOut.set(v);
        context.write(keyOut, valueOut);

    }
}
