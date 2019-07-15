package com.xq.learn.mapreduce.sortkey.driver;

import com.xq.learn.mapreduce.sortkey.group.MyGroupingComparator;
import com.xq.learn.mapreduce.sortkey.map.FlowMapper;
import com.xq.learn.mapreduce.sortkey.model.FlowBean;
import com.xq.learn.mapreduce.sortkey.reduce.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 使用自定义Bean作为MapReduce的key，必须实现WritableComparable接口
 * 如果没有指定分区，还必须重写hashCode()方法
 * @author xiaoqiang
 * @date 2019/7/14 13:21
 */
public class FlowSort
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, FlowSort.class.getSimpleName());

        job.setJarByClass(FlowSort.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(Text.class);

        // 设置自定义分组函数, 分组是在对key排序的基础之上进行分组，按顺序进行遍历，遇到相同的key就划分为一组
        // 如果排序阶段相同的组不是连续的，则分组之后不会将其划分到同一个组内
        job.setGroupingComparatorClass(MyGroupingComparator.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output))
        {
            fs.delete(output, true);
            fs.close();
        }

        boolean wait = job.waitForCompletion(true);

        System.exit(wait ? 0 : 1);
    }
}
