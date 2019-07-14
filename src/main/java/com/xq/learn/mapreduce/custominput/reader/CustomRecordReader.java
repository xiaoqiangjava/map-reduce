package com.xq.learn.mapreduce.custominput.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义RecordReader，一次读取文件的全部内容
 * @author xiaoqiang
 * @date 2019/7/14 0:54
 */
public class CustomRecordReader extends RecordReader<Text, BytesWritable>
{
    private Text key = new Text();

    private BytesWritable value = new BytesWritable();

    private Configuration conf;

    private FileSplit fileSplit;

    private FileSystem fs;

    private FSDataInputStream input;

    private boolean processed = false;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException
    {
        this.conf = context.getConfiguration();
        this.fileSplit = (FileSplit) inputSplit;
        this.key.set(fileSplit.getPath().getName());
        this.fs = FileSystem.get(conf);
        this.input = fs.open(this.fileSplit.getPath());
    }

    @Override
    public boolean nextKeyValue() throws IOException
    {
        if (!processed)
        {
            byte[] buff = new byte[(int) fileSplit.getLength()];
            IOUtils.readFully(input, buff, 0, buff.length);
            this.value.set(buff, 0, buff.length);
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey()
    {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue()
    {
        return value;
    }

    @Override
    public float getProgress()
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {
        this.input.close();
        this.fs.close();
    }
}
