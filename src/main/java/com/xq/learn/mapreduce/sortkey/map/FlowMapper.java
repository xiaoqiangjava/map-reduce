package com.xq.learn.mapreduce.sortkey.map;

import com.xq.learn.mapreduce.sortkey.model.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义Bean作为key写出
 * @author xiaoqiang
 * @date 2019/7/14 13:11
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text>
{
    private FlowBean keyOut = new FlowBean();

    private Text valueOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] fields = line.split(" ");
        String phone = fields[0];
        long upload = Long.parseLong(fields[1]);
        long download = Long.parseLong(fields[2]);
        keyOut.set(upload, download);
        valueOut.set(phone);
        context.write(keyOut, valueOut);
    }
}
