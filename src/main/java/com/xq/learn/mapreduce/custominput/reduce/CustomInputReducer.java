package com.xq.learn.mapreduce.custominput.reduce;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/7/14 1:27
 */
public class CustomInputReducer extends Reducer<Text, BytesWritable, Text, BytesWritable>
{
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException
    {
        context.write(key, values.iterator().next());
    }
}
