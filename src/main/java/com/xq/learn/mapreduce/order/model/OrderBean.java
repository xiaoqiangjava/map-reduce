package com.xq.learn.mapreduce.order.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/7/14 16:10
 */
public class OrderBean implements WritableComparable<OrderBean>
{
    private long orderId;

    private double price;

    public void set(long orderId, double price)
    {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean that)
    {
        // 首先根据订单id排序，当订单id相同时，按照价格降序排序
        int result = Long.compare(this.orderId, that.orderId);
        return result == 0 ? Double.compare(this.price, that.price) : result;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeLong(this.orderId);
        out.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.orderId = in.readLong();
        this.price = in.readDouble();
    }

    public long getOrderId()
    {
        return orderId;
    }

    public void setOrderId(long orderId)
    {
        this.orderId = orderId;
    }

    public double getPrice()
    {
        return price;
    }

    public void setPrice(double price)
    {
        this.price = price;
    }

    @Override
    public String toString()
    {
        return orderId + " " + price;
    }
}
