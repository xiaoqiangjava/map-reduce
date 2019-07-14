package com.xq.learn.mapreduce.nlineinput.driver;

import com.xq.learn.mapreduce.nlineinput.map.NLineMapper;
import com.xq.learn.mapreduce.nlineinput.reduce.NLineReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 使用NLineInputFormat读取文件，按照行划分文件切片
 * @author xiaoqiang
 * @date 2019/7/13 16:31
 */
public class NLineWordCount
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        // 1. 获取Job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, NLineWordCount.class.getSimpleName());
        // 2. 设置程序运行的jar
        job.setJarByClass(NLineWordCount.class);
        // 3. 关联Mapper和Reducer
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReduce.class);
        // 4. 设置Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 5. 设置Reducer输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 6. 设置InputFormat类型为NLineInputFormat
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 2);
        // 7. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 8. 提交作业
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
