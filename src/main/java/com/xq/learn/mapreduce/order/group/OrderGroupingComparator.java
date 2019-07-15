package com.xq.learn.mapreduce.order.group;

import com.xq.learn.mapreduce.order.model.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组，将订单id相同的数组分为一组，value组装成一个Iterator
 * 当自定义了分组函数后，会将第一个key穿给reduce()方法
 * @author xiaoqiang
 * @date 2019/7/14 17:03
 */
public class OrderGroupingComparator extends WritableComparator
{
    public OrderGroupingComparator()
    {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b)
    {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        return Long.compare(aBean.getOrderId(), bBean.getOrderId());
    }
}
