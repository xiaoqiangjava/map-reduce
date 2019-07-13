package com.xq.learn.mapreduce.flowcount.reduce;

import com.xq.learn.mapreduce.flowcount.model.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ForkJoinPool;

/**
 * @author xiaoqiang
 * @date 2019/7/12 1:19
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean>
{
    private FlowBean outValue = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException
    {
        long sumUpload = 0;
        long sumDownload = 0;
        Iterator<FlowBean> iterator = values.iterator();
        while (iterator.hasNext())
        {
            FlowBean flow = iterator.next();
            sumUpload += flow.getUpload();
            sumDownload += flow.getDownload();
        }
        outValue.set(sumUpload, sumDownload);
        context.write(key, outValue);
    }
}
