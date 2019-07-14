package com.xq.learn.mapreduce.custominput.driver;

import com.xq.learn.mapreduce.custominput.input.CustomInputFormat;
import com.xq.learn.mapreduce.custominput.mapper.CustomInputMapper;
import com.xq.learn.mapreduce.custominput.reduce.CustomInputReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/7/14 1:29
 */
public class CustomInputDriver
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 设置jar包
        job.setJarByClass(CustomInputDriver.class);
        // 关联Mapper和Reducer
        job.setMapperClass(CustomInputMapper.class);
        job.setReducerClass(CustomInputReducer.class);
        // 设置Mapper和Reducer输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        // 设置InputFormat
        job.setInputFormatClass(CustomInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交作业
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
