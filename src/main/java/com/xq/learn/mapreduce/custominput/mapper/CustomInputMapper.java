package com.xq.learn.mapreduce.custominput.mapper;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/7/14 1:26
 */
public class CustomInputMapper extends Mapper<Text, BytesWritable, Text, BytesWritable>
{
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException
    {
        context.write(key, value);
    }
}
