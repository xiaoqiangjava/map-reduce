package com.xq.learn.mapreduce.nlineinput.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/7/13 16:29
 */
public class NLineReduce extends Reducer<Text, LongWritable, Text, LongWritable>
{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for (LongWritable value : values)
        {
            sum += value.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
