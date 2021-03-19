package com.leejean.tag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TagReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable value : values){
            total = total + value.get();
        }
        context.write(new Text(key.toString()), new IntWritable(total));
    }

    }

