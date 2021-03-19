package com.leejean.tag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TagMain1 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(TagMain1.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path("file:///E:\\tmp\\temptags.txt"));
        FileOutputFormat.setOutputPath(job, new Path("file:///E:\\tmp\\out"));

        job.setMapperClass(TagMapper1.class);
        job.setReducerClass(TagReducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setPartitionerClass(TagPartitioner.class);
//        job.setGroupingComparatorClass(TagContentGroupComparator.class);

        //job.setSortComparatorClass(ComboKeyComparator.class);

        job.setNumReduceTasks(2);
        /*
        第一步的输出：
        77287793,音响效果好	26
        77287793,音响效果差	1
        77287793,高大上	16
         */

        //第二次的mapreduce任务
        boolean b = job.waitForCompletion(true);

        //第一个重点在这里：原来是如果任务成功就退出，现在直接进行下一次计算
        if (b == true) {
            //重点二在这里：所有的对象最好不要使用同一个，或者把对象以类成员的形式提出去也行
            Configuration cfg1 = new Configuration();
            //获取到任务
            Job job1 = Job.getInstance(cfg1);
            job1.setJarByClass(TagMain1.class);

            FileInputFormat.setInputPaths(job1, new Path("file:///E:\\tmp\\out"));
            FileOutputFormat.setOutputPath(job1, new Path("file:///E:\\tmp\\out1"));
            //设置map reduce类
            job1.setMapperClass(TagMapper2.class);
            job1.setReducerClass(TagReducer2.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);


            //对输出参数设置
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(NullWritable.class);

            //重点三在这里：嵌套时当前MR的输入路径如果需要上一次MR的数据那么必须使用上一次的输出路径，不能使用输入路径
            //设置输入输出路径

            boolean b1 = job1.waitForCompletion(true);

            //重点四在这里：如果后面不用继续嵌套MR则正常老方法结束，如果有同样的方法继续嵌套
            System.exit(b1 == true ? 0 : -1);


        }
    }
}
