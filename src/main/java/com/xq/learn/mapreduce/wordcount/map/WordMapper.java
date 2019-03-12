package com.xq.learn.mapreduce.wordcount.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map函数
 * @author xiaoqiang
 * @date 2019/3/7 0:01
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
    private Text k2 = new Text();

    private LongWritable v2 = new LongWritable();

    /**
     * 对文件中的每一行进行处理, 每一行数据都会调用一次map函数
     * @param key 输入的key
     * @param value 输入的value
     * @param context context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        System.out.println("<k1, v1>: <" + key.get() + "," + value.toString() + ">");
        // 每个单词出现一次
        String[] words = line.split(" ");
        for (String word : words)
        {
            k2.set(word);
            v2.set(1);
            System.out.println("<k2, v2>: <" + k2.toString() + "," + v2.get() + ">");
            context.write(k2, v2);
        }
    }
}
