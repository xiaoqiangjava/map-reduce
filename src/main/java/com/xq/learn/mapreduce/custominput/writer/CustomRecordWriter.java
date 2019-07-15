package com.xq.learn.mapreduce.custominput.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * 自定义输出流
 * @author xiaoqiang
 * @date 2019/7/16 1:38
 */
public class CustomRecordWriter extends RecordWriter<Text, NullWritable>
{
    private FileSystem fs;

    private FSDataOutputStream output;

    public CustomRecordWriter(TaskAttemptContext context) throws IOException
    {
        Configuration conf = context.getConfiguration();
        fs = FileSystem.get(conf);
        output = fs.create(new Path(""));
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException
    {
        output.write(new byte[10]);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException
    {
        output.close();
        fs.close();
    }
}
