package com.xq.learn.mapreduce.custominput.input;

import com.xq.learn.mapreduce.custominput.reader.CustomRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 自定义FileInputFormat: 实现一次读取一个小文件的全部内容，K是文件名，V是文件内容
 * @author xiaoqiang
 * @date 2019/7/14 0:51
 */
public class CustomInputFormat extends FileInputFormat
{
    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    {
        return new CustomRecordReader();
    }
}
