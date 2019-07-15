package com.xq.learn.mapreduce.sortkey.group;

import com.xq.learn.mapreduce.sortkey.model.FlowBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组函数：这里的分组不是按照equals方法比较相等的分为一组，而是通过Comparator比较相等
 * 自定义分组函数，需要继承WritableComparator，重写compare方法
 * @author xiaoqiang
 * @date 2019/7/14 15:22
 */
public class MyGroupingComparator extends WritableComparator
{
    public MyGroupingComparator()
    {
        super(FlowBean.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b)
    {
        FlowBean x = (FlowBean) a;
        FlowBean y = (FlowBean) b;
        return Long.compare(x.getUpload(), y.getUpload());
    }
}
