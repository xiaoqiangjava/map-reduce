package com.xq.learn.mapreduce.wccombineinput.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 用户自定义的Mapper要继承Mapper父类
 * Mapper的输入数据是KV格式，KV的类型可以自定义
 * Mapper中的业务逻辑写在map()方法中
 * Mapper的输出数据是KV格式，KV的类型可以自定义
 * map()方法(map task进程)对每一个<K, V>调用一次
 * @author xiaoqiang
 * @date 2019/7/11 1:40
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private Text keyOut = new Text();

    private IntWritable valueOut = new IntWritable(1);

    /**
     * map()方法对每一个<K, V>调用一次
     * @param key key
     * @param value value
     * @param context context
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // 1. 将每一行数据转换成String
        String line = value.toString();
        // 2. 按照空格将单词切割
        String[] words = line.split(" ");
        // 3. 统计每个单词出现的次数
        for (String word : words)
        {
            keyOut.set(word);
            context.write(keyOut, valueOut);
        }
    }
}
