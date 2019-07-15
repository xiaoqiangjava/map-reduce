package com.xq.learn.mapreduce.partitioner.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/7/14 12:30
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private IntWritable one = new IntWritable(1);

    private Text keyOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words)
        {
            keyOut.set(word);
            context.write(keyOut, one);
        }
    }
}
