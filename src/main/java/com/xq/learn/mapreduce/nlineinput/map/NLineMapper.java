package com.xq.learn.mapreduce.nlineinput.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 使用NLineInputFormat实现单词统计
 * @author xiaoqiang
 * @date 2019/7/13 16:25
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
    private LongWritable one = new LongWritable(1);

    private Text keyOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // 获取每一行数据
        String line = value.toString();
        // 获取每个单词
        String[] words = line.split(" ");
        // 统计每个单词出现的次数
        for (String word : words)
        {
            keyOut.set(word);
            context.write(keyOut, one);
        }
    }
}
