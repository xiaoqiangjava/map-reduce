package com.xq.learn.mapreduce.order.partitioner;

import com.xq.learn.mapreduce.order.model.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author xiaoqiang
 * @date 2019/7/14 16:36
 */
public class OrderPartitioner extends Partitioner<OrderBean, NullWritable>
{
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTasks)
    {
        Long orderId = orderBean.getOrderId();
        return (orderId.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
