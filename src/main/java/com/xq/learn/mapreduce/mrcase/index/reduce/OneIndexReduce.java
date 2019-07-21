package com.xq.learn.mapreduce.mrcase.index.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 聚合没对KV
 * hello-->a.txt    2
 * hello-->b.txt    2
 * @author xiaoqiang
 * @date 2019/7/21 13:57
 */
public class OneIndexReduce extends Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        // 聚合
        int sum = 0;
        for (IntWritable value : values)
        {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
