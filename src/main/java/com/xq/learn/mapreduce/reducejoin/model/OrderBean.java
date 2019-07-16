package com.xq.learn.mapreduce.reducejoin.model;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * MapReduce任务join操作，将订单表数据和商品表数据做join，在reduce端实现
 * 订单表：
 * orderId  pId price
 * 000001   1   34
 * 000001   2   56
 * 000002   1   98
 * 000002   2   87
 * 商品表：
 * pId  pName
 * 1    耳机
 * 2    蓝牙耳机
 * @author xiaoqiang
 * @date 2019/7/16 23:10
 */
public class OrderBean implements WritableComparable<OrderBean>
{
    private String orderId = "";

    private int pId;

    private String pName = "";

    private double price;

    // 标志位：0表示数据来源于订单表，1表示数据来源于商品表
    private int flag;

    @Override
    public int compareTo(OrderBean that)
    {
        if (this.pId == that.pId)
        {
            return this.orderId.compareTo(that.getOrderId());
        }
        return Integer.compare(this.pId, that.pId);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(orderId);
        out.writeInt(pId);
        out.writeUTF(pName);
        out.writeDouble(price);
        out.writeInt(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.orderId = in.readUTF();
        this.pId = in.readInt();
        this.pName = in.readUTF();
        this.price = in.readDouble();
        this.flag = in.readInt();
    }

    public String getOrderId()
    {
        return orderId;
    }

    public void setOrderId(String orderId)
    {
        this.orderId = orderId;
    }

    public int getpId()
    {
        return pId;
    }

    public void setpId(int pId)
    {
        this.pId = pId;
    }

    public String getpName()
    {
        return pName;
    }

    public void setpName(String pName)
    {
        this.pName = pName;
    }

    public double getPrice()
    {
        return price;
    }

    public void setPrice(double price)
    {
        this.price = price;
    }

    public int getFlag()
    {
        return flag;
    }

    public void setFlag(int flag)
    {
        this.flag = flag;
    }

    @Override
    public String toString()
    {
        return pId + "\t" + orderId + "\t" + pName + "\t" + price;
    }
}
