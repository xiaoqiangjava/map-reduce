package com.xq.learn.mapreduce.reducejoin.sort;

import com.xq.learn.mapreduce.reducejoin.model.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author xiaoqiang
 * @date 2019/7/17 1:11
 */
public class JoinSort extends WritableComparator
{
    public JoinSort()
    {
        super(OrderBean.class, true);
    }
    @Override

    public int compare(WritableComparable a, WritableComparable b)
    {
        return ((OrderBean) a).getOrderId().compareTo(((OrderBean) b).getOrderId());
    }
}
