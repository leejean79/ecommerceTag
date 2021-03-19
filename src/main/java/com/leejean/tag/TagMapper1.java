package com.leejean.tag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
77287793	{"reviewPics":null,"extInfoList":[{"title":"contentTags","values":["干净卫生","服务热情"],"desc":"","defineType":0},{"title":"tagIds","values":["852","22"],"desc":"","defineType":0}],"expenseList":null,"reviewIndexes":[1,2],"scoreList":null}

 */

public class TagMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    //map输出：(77287793, 干净卫生), 1
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Text text = new Text();

        String line = value.toString();
        String[] arr = line.split("\t");

        //每条日志
        if (arr.length != 2){
            return;
        }

        //tags为"干净卫生,服务热情"
        String tags = ReviewTags.extractTags(arr[1]);
        if (tags == ""){
            return;
        }
        String[] tagArr = tags.split(",");
        if (tagArr.length > 0 ){
            for (int i = 0; i < tagArr.length; i ++){
                if (tagArr[i].length() != 0){
                    //
                    // 封装key："77287793, 干净卫生"
                    text.set(arr[0] + "," + tagArr[i]);
                    context.write(text, new IntWritable(1));
                }

            }
        }


    }
}
