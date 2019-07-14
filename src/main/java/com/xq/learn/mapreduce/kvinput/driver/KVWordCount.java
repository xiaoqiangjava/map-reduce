package com.xq.learn.mapreduce.kvinput.driver;

import com.xq.learn.mapreduce.kvinput.mapper.KVMapper;
import com.xq.learn.mapreduce.kvinput.reduce.KVReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计文件每一行第一个单词出现的次数
 * @author xiaoqiang
 * @date 2019/7/13 16:00
 */
public class KVWordCount
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        // 1. 创建配置对象，设置KeyValue分割符, 该操作必须在获取job实例之前
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        // 2. 获取job实例
        Job job = Job.getInstance(conf, KVWordCount.class.getSimpleName());
        // 3. 指定程序运行的jar包
        job.setJarByClass(KVWordCount.class);
        // 4. 关联Mapper和Reducer类
        job.setMapperClass(KVMapper.class);
        job.setReducerClass(KVReducer.class);
        // 5. 设置Mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 6. 设置Reducer的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 7. 指定读取文件的InputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        // 8. 指定文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 9. 提交作业
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
