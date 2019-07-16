package com.xq.learn.mapreduce.reducejoin.driver;

import com.xq.learn.mapreduce.reducejoin.map.JoinMapper;
import com.xq.learn.mapreduce.reducejoin.model.OrderBean;
import com.xq.learn.mapreduce.reducejoin.reduce.JoinReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/7/17 0:03
 */
public class JoinDriver
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, JoinDriver.class.getSimpleName());
        job.setJarByClass(JoinDriver.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(OrderBean.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileSystem fs = FileSystem.get(conf);
        Path out = new Path(args[1]);
        if (fs.exists(out))
        {
            fs.delete(out, true);
        }
        FileOutputFormat.setOutputPath(job, out);

        boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0 : 1);
    }
}
