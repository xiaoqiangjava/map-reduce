package com.xq.learn.mapreduce.wccombineinput.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 用户自定义的Reduce要继承Reducer父类
 * Reducer的输入类型对应Mapper阶段的输出数据类型，也是KV
 * Reducer的业务逻辑卸载reduce()方法中
 * reduce()方法(reduce task进程)对每一个相同K的<K, V>调用一次
 * @author xiaoqiang
 * @date 2019/7/11 1:51
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    /**
     * 每一相同Key的<K, V>调用一次
     * @param key word
     * @param values count list
     * @param context context
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        // reduce
        Iterator<IntWritable> iterator = values.iterator();
        int sum = 0;
        while (iterator.hasNext())
        {
            sum += iterator.next().get();
        }
        context.write(key, new IntWritable(sum));
    }
}
