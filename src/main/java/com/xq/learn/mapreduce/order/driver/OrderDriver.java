package com.xq.learn.mapreduce.order.driver;

import com.xq.learn.mapreduce.order.group.OrderGroupingComparator;
import com.xq.learn.mapreduce.order.map.OrderMapper;
import com.xq.learn.mapreduce.order.model.OrderBean;
import com.xq.learn.mapreduce.order.partitioner.OrderPartitioner;
import com.xq.learn.mapreduce.order.reduce.OrderReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 获取同一个订单中最高的价格
 * @author xiaoqiang
 * @date 2019/7/14 16:39
 */
public class OrderDriver
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        // 获取Job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, OrderDriver.class.getSimpleName());

        // 设置程序jar包
        job.setJarByClass(OrderDriver.class);
        // 关联Mapper和Reducer
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
        // 设置Mapper和最终的输入输出
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置分区
        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);

        // 设置区内分组函数
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        // 设置输入输出路径
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
