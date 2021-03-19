package com.leejean.tag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/*
输入：
77287793    “高大上:16”
输出：
77287793  “服务热情:41,干净卫生:23”
 */

public class TagReducer2 extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> iter = values.iterator();
        Text text = new Text();
        NullWritable nullWritable = NullWritable.get();

        StringBuilder sb = new StringBuilder();
        sb.append(key.toString());
        sb.append("\t");
        //实现先插入一个标签，紧接着一个逗号，并且末尾没有逗号
        boolean begin = true;
        while (iter.hasNext()){
            String line = iter.next().toString();
            if (begin) {
                begin = false;
            } else {
                sb.append(",");
            }
            sb.append(line);
        }

        text.set(sb.toString());
        context.write(text, nullWritable);



    }
}

