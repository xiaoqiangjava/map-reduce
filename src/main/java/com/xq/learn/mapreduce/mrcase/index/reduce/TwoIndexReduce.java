package com.xq.learn.mapreduce.mrcase.index.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/7/21 14:21
 */
public class TwoIndexReduce extends Reducer<Text, Text, Text, Text>
{
    private Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        StringBuilder builder = new StringBuilder();
        for (Text value : values)
        {
            builder.append(value.toString()).append("\t");
        }
        outVal.set(builder.toString().trim());
        context.write(key, outVal);
    }
}
