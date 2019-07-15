package com.xq.learn.mapreduce.custominput.output;

import com.xq.learn.mapreduce.custominput.writer.CustomRecordWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义输出类
 * @author xiaoqiang
 * @date 2019/7/16 1:36
 */
public class CustomOutput extends FileOutputFormat<Text, NullWritable>
{
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new CustomRecordWriter(context);
    }
}
