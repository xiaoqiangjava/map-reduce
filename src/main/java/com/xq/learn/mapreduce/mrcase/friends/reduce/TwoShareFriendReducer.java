package com.xq.learn.mapreduce.mrcase.friends.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/7/21 16:00
 */
public class TwoShareFriendReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        StringBuilder builder = new StringBuilder();
        for (Text value : values)
        {
            builder.append(value.toString()).append(" ");
        }
        context.write(key, new Text(builder.toString().trim()));
    }
}
