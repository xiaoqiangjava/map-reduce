package com.xq.learn.mapreduce.order.map;

import com.xq.learn.mapreduce.order.model.OrderBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 获取每个订单中最高的价格
 * 000001 pdt_000001 23.50
 * @author xiaoqiang
 * @date 2019/7/14 16:10
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>
{
    private OrderBean orderBean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] fields = line.split(" ");
        long orderId = Long.parseLong(fields[0]);
        double price = Double.parseDouble(fields[2]);
        orderBean.set(orderId, price);
        context.write(orderBean, NullWritable.get());
    }
}
