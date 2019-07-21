package com.xq.learn.mapreduce.mrcase.index.driver;

import com.xq.learn.mapreduce.mrcase.index.map.TwoIndexMapper;
import com.xq.learn.mapreduce.mrcase.index.reduce.TwoIndexReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/7/21 14:25
 */
public class TwoIndexDriver
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        // 创建job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, TwoIndexDriver.class.getSimpleName());

        // 设置运行jar
        job.setJarByClass(TwoIndexDriver.class);

        // 关联Mapper和Reducer
        job.setMapperClass(TwoIndexMapper.class);
        job.setReducerClass(TwoIndexReduce.class);

        // 设置输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置InputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 设置输入输出路径
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output))
        {
            fs.delete(output, true);
        }
        fs.close();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);

        // 提交作业
        boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0 : 1);
    }
}
