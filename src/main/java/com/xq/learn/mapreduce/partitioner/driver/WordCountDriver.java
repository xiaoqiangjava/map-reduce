package com.xq.learn.mapreduce.partitioner.driver;

import com.xq.learn.mapreduce.partitioner.custom.CustomPartitioner;
import com.xq.learn.mapreduce.partitioner.map.WordCountMapper;
import com.xq.learn.mapreduce.partitioner.reduce.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义分区实现单词计数，将A-Q分到一个分区，a-q分到一个分区，Q-Z一个分区，q-z一个分区
 * @author xiaoqiang
 * @date 2019/7/14 12:36
 */
public class WordCountDriver
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        // 获取Job实例
        Configuration conf = new Configuration();
        // 启用map端输出结果压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        // 设置输出压缩，也可以在outputformat中设置
//        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
//        conf.setClass("mapreduce.output.fileoutputformat.compress.codec", GzipCodec.class, CompressionCodec.class);
//        // 设置按块压缩，默认是按行压缩
//        conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
        Job job = Job.getInstance(conf, WordCountDriver.class.getSimpleName());
        // 设置主程序jar包
        job.setJarByClass(WordCountDriver.class);
        // 关联Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 设置Mapper和Reducer的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置分区
        job.setPartitionerClass(CustomPartitioner.class);
        // 设置reduce task任务数量, 自定义分区之后必须设置对应的reduce task的数量才能使分区生效，否则所有的分区数据都写到同一个文件
        job.setNumReduceTasks(5);
        job.setCombinerClass(WordCountReducer.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output))
        {
            fs.delete(output, true);
            fs.close();
        }

        // 提交作业
        boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0 : 1);
    }
}
