package com.xq.learn.mapreduce.kvinput.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 使用<code>KeyValueTextInputFormat</code>读取文件内容
 * 需求：统计每一行的第一个单词出现的次数
 * @author xiaoqiang
 * @date 2019/7/13 15:50
 */
public class KVMapper extends Mapper<Text, Text, Text, LongWritable>
{
    private LongWritable one = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException
    {
        context.write(key, one);
    }
}
