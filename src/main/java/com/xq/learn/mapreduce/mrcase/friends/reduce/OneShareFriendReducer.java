package com.xq.learn.mapreduce.mrcase.friends.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 将A是谁的好友聚合
 * A    B
 * A    C
 * 输出：
 * A    B,C
 * @author xiaoqiang
 * @date 2019/7/21 15:29
 */
public class OneShareFriendReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        StringBuilder builder = new StringBuilder();
        for (Text value : values)
        {
            builder.append(value.toString()).append(",");
        }
        context.write(key, new Text(builder.toString()));
    }
}
