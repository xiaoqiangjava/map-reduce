package com.xq.learn.mapreduce.flowcount.map;

import com.xq.learn.mapreduce.flowcount.model.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计每个手机的上行流量，下行流量以及流量总和：15093232134   2048    102400
 * @author xiaoqiang
 * @date 2019/7/12 1:10
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>
{
    private Text outKey = new Text();

    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // 获取数据
        String line = value.toString();
        // 切割，获取手机号，上行流量，下行流程，流量综合
        String[] fields = line.split("\\t");
        String phone = fields[0];
        long upload = Long.parseLong(fields[1]);
        long download = Long.parseLong(fields[2]);
        flow.set(upload, download);
        outKey.set(phone);
        context.write(outKey, flow);
    }
}
