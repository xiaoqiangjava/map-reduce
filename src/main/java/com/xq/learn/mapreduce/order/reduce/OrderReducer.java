package com.xq.learn.mapreduce.order.reduce;

import com.xq.learn.mapreduce.order.model.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 当设置了自定义分组函数后，排序之后的第一个key会穿给reduce()方法
 * @author xiaoqiang
 * @date 2019/7/14 16:34
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>
{
    private static final Logger logger = LoggerFactory.getLogger(OrderReducer.class);
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
    {
        logger.info(">>>>>>>>>>>>>>>>" + key.toString());
        context.write(key, values.iterator().next());
    }
}
