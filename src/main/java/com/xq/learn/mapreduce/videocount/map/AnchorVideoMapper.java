package com.xq.learn.mapreduce.videocount.map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xq.learn.mapreduce.videocount.model.VideoInfoWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 主播视频统计mapper
 * @author xiaoqiang
 * @date 2019/3/11 23:39
 */
public class AnchorVideoMapper extends Mapper<LongWritable, Text, Text, VideoInfoWritable>
{
    private Text k2 = new Text();

    private VideoInfoWritable v2 = new VideoInfoWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        System.out.println("line:" + line);
        JSONObject lineObj = JSON.parseObject(line);
        k2.set(lineObj.getString("uid"));
        v2.set(lineObj.getLong("gold"), lineObj.getLong("watchNumPV"), lineObj.getLong("follower"), lineObj.getLong("duration"));
        context.write(k2, v2);
    }
}
