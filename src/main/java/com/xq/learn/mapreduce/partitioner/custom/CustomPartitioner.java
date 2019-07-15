package com.xq.learn.mapreduce.partitioner.custom;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author xiaoqiang
 * @date 2019/7/14 12:17
 */
public class CustomPartitioner extends Partitioner<Text, IntWritable>
{
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i)
    {
        String key = text.toString();
        if (key.length() < 1)
        {
            return 0;
        }
        char first = key.charAt(0);
        // 首字母为大写字母
        if (first < 'a')
        {
            if (first >= 'A' && first < 'Q')
            {
                return 1;
            }
            else
            {
                return 2;
            }
        }
        else
        {
            if (first >= 'a' && first < 'q')
            {
                return 3;
            }
            else
            {
                return 4;
            }
        }
    }
}
