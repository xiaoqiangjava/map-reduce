package com.xq.learn.mapreduce.mrcase.index.driver;

import com.xq.learn.mapreduce.mrcase.index.map.OneIndexMapper;
import com.xq.learn.mapreduce.mrcase.index.reduce.OneIndexReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 需求：有大量的文本，需要建立搜索索引
 * a.txt
 * hello    python
 * hello    world
 * b.txt
 * hello    java
 * hello    python
 * 结果：
 * hello    a.txt--2    b.txt--2
 * python   a.txt--1    b.txt--1
 * @author xiaoqiang
 * @date 2019/7/21 13:42
 */
public class OneIndexDriver
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        // 创建job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, OneIndexDriver.class.getSimpleName());

        // 设置jar
        job.setJarByClass(OneIndexDriver.class);

        // 关联Mapper和Reducer
        job.setMapperClass(OneIndexMapper.class);
        job.setReducerClass(OneIndexReduce.class);

        // 设置Mapper和Reducer输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入输出目录
        FileSystem fs = FileSystem.get(conf);
        Path output = new Path(args[1]);
        if (fs.exists(output))
        {
            fs.delete(output, true);
        }
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);


        // 提交作业
        boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0 : 1);
    }
}
