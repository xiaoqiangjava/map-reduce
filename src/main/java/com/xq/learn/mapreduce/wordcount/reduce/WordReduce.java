package com.xq.learn.mapreduce.wordcount.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * reduce函数
 * @auth xiaoqiang
 * @data 2019/3/6 23:58
 */
public class WordReduce extends Reducer<Text, LongWritable, Text, LongWritable>
{
    private Text k3 = new Text();

    private LongWritable v3 = new LongWritable();

    /**
     * 针对每一个<key, vaues>都会调用一次reduce函数
     * @param key 输入key
     * @param values 输入values
     * @param context context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
    {
        Iterator<LongWritable> iterator = values.iterator();
        System.out.println("<k3, v3>: <" + key.toString() + "," + values.toString() + ">");
        long sum = 0L;
        while (iterator.hasNext())
        {
            LongWritable count = iterator.next();
            sum += count.get();
        }
        k3.set(key);
        v3.set(sum);
        System.out.println("<k4, v4>: <" + k3.toString() + "," + v3.get() + ">");
        context.write(k3, v3);
    }
}
