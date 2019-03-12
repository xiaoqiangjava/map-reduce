package com.xq.learn.mapreduce.wordcount.main;

import com.xq.learn.mapreduce.wordcount.map.WordMapper;
import com.xq.learn.mapreduce.wordcount.reduce.WordReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 单词计数
 * @author xiaoqiang
 * @date 2019/3/7 0:01
 */
public class WordCount
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        if (args.length < 2)
        {
            System.exit(-1);
        }
        // 0. 获取程序的输入和输入参数
        String inputPath = args[0];
        String outputPath = args[1];
        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + outputPath);
        // 1. 获取配置类
        Configuration conf = new Configuration();
        // 2. 获取job
        String jobName = WordCount.class.getSimpleName();
        Job job = Job.getInstance(conf, jobName);
        // 3. 组装jar包必备代码
        job.setJarByClass(WordCount.class);
        // 4. 设置输入目录
        FileInputFormat.setInputPaths(job, inputPath);
        // 5. 设置输出目录
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // 6. 设置map相关参数
        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 7. 设置reduce相关参数
        job.setReducerClass(WordReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 8. 提交job, 等待任务完成
        job.waitForCompletion(true);
    }
}
