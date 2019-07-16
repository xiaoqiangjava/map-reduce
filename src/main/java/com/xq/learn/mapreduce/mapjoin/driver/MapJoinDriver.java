package com.xq.learn.mapreduce.mapjoin.driver;

import com.xq.learn.mapreduce.mapjoin.map.JoinMapper;
import com.xq.learn.mapreduce.reducejoin.model.OrderBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @author xiaoqiang
 * @date 2019/7/17 2:33
 */
public class MapJoinDriver
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, MapJoinDriver.class.getSimpleName());
        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(JoinMapper.class);
        // 由于不需要reduce阶段，所以不需要指定Mapper的输出类
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        job.addCacheFile(URI.create("file:///D:/data/input/order/product.txt"));

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
