package com.xq.learn.mapreduce.reducejoin.reduce;

import com.xq.learn.mapreduce.reducejoin.model.OrderBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.hbase.types.OrderedFloat32;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * 将同一个pId的数据关联查询，将订单表中的pId替换成商品表中的pName
 * 000001   耳机  23
 * @author xiaoqiang
 * @date 2019/7/16 23:40
 */
public class JoinReduce extends Reducer<IntWritable, OrderBean, OrderBean, NullWritable>
{
    private static final int ORDER_FLAG = 0;

    private List<OrderBean> orders = new ArrayList<>();

    private OrderBean product = new OrderBean();

    @Override
    protected void reduce(IntWritable key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException
    {
        try
        {
            // 遍历的时候需要注意：每次拿到的value是同一个实例，因为调用next()方法时是对value实例赋值
            for (OrderBean orderBean : values)
            {
                // 将订单数据封装成list
                if (orderBean.getFlag() == ORDER_FLAG)
                {
                    OrderBean order = new OrderBean();
                    BeanUtils.copyProperties(order, orderBean);
                    orders.add(order);
                }
                else
                {
                    BeanUtils.copyProperties(product, orderBean);
                }
            }
        }
        catch (IllegalAccessException | InvocationTargetException e)
        {
            e.printStackTrace();
        }

        for (OrderBean orderBean : orders)
        {
            orderBean.setpName(product.getpName());
            context.write(orderBean, NullWritable.get());
        }
        orders.clear();
    }
}
