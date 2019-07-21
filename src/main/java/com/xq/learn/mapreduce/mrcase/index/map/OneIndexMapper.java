package com.xq.learn.mapreduce.mrcase.index.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 统计每个文档中出现的单词的次数：
 * hello--a.txt 1
 * hello--b.txt 1
 * @author xiaoqiang
 * @date 2019/7/21 13:40
 */
public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private static final String KEY_SEP = "-->";

    private Text keyOut = new Text();

    private IntWritable valOut = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // 1. 获取文件名
        FileSplit split = (FileSplit) context.getInputSplit();
        String fileName = split.getPath().getName();
        // 2. 读取文件中的内容，封装成<word--fileName, 1>
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words)
        {
            String keyStr = word + KEY_SEP + fileName;
            keyOut.set(keyStr);
            context.write(keyOut, valOut);
        }
    }
}
