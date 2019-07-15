package com.xq.learn.mapreduce.sortkey.reduce;

import com.xq.learn.mapreduce.sortkey.model.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/7/14 13:19
 */
public class FlowReducer extends Reducer<FlowBean, Text, FlowBean, Text>
{
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        Text valOut = new Text();
        for (Text text : values)
        {
            valOut.set(valOut.toString() + ":" + text.toString());
        }
        context.write(key, valOut);
    }
}
