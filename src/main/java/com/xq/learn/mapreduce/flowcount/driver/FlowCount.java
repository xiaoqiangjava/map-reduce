package com.xq.learn.mapreduce.flowcount.driver;

import com.xq.learn.mapreduce.flowcount.map.FlowMapper;
import com.xq.learn.mapreduce.flowcount.model.FlowBean;
import com.xq.learn.mapreduce.flowcount.reduce.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计每个手机的上行流量，下行流量以及流量总和：15093232134   2048    102400
 * 使用默认的InputFormat：TextInputFormat
 * @author xiaoqiang
 * @date 2019/7/12 1:27
 */
public class FlowCount
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        // 1. 获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, FlowCount.class.getSimpleName());
        // 2. 指定jar包路径
        job.setJarByClass(FlowCount.class);
        // 3. 关联Mapper和Reducer业务类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        // 4. 指定Mapper的输出KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 5. 指定Reducer的输出KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 6. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 7. 提交作业
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
