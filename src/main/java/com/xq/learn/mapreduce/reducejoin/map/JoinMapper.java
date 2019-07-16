package com.xq.learn.mapreduce.reducejoin.map;

import com.xq.learn.mapreduce.reducejoin.model.OrderBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * map端将连接字段作为key传递给reduce端，保证同一个pId对应的订单数据和商品数据同时传到reduce方法
 * @author xiaoqiang
 * @date 2019/7/16 23:23
 */
public class JoinMapper extends Mapper<LongWritable, Text, IntWritable, OrderBean>
{
    private static final String ORDER_FILE_NAME = "order";

    private static final int ORDER_FLAG = 0;

    private static final int PRODUCT_FLAG = 1;

    private OrderBean outVal = new OrderBean();

    private IntWritable outKey = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // 获取两张表中的数据
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        if (StringUtils.startsWith(fileName, ORDER_FILE_NAME))
        {
            // 数据来源于订单表，封装订单bean
            String[] fields = value.toString().split("\t");
            outVal.setOrderId(fields[0]);
            outVal.setPrice(Double.parseDouble(fields[2]));
            outVal.setFlag(ORDER_FLAG);
            int pid = Integer.parseInt(fields[1]);
            outVal.setpId(pid);
            outKey.set(pid);
            context.write(outKey, outVal);
        }
        else
        {
            // 数据来源于商品表，封装商品bean
            String[] fields = value.toString().split("\t");
            int pid = Integer.parseInt(fields[0]);
            outVal.setpName(fields[1]);
            outVal.setFlag(PRODUCT_FLAG);
            outKey.set(pid);
            context.write(outKey, outVal);
        }
    }
}
