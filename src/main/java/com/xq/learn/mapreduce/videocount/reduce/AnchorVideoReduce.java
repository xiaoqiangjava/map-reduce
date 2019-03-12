package com.xq.learn.mapreduce.videocount.reduce;

import com.xq.learn.mapreduce.videocount.model.VideoInfoWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/3/11 23:41
 */
public class AnchorVideoReduce extends Reducer<Text, VideoInfoWritable, Text, VideoInfoWritable>
{
    private VideoInfoWritable v3 = new VideoInfoWritable();

    @Override
    protected void reduce(Text key, Iterable<VideoInfoWritable> values, Context context) throws IOException, InterruptedException
    {
        Long gold = 0L;
        Long watchNumPV = 0L;
        Long followerNum = 0L;
        Long duration = 0L;
        for (VideoInfoWritable videoInfo : values)
        {
            gold += videoInfo.getGold();
            watchNumPV += videoInfo.getWatchNumPV();
            followerNum += videoInfo.getFollowerNum();
            duration += videoInfo.getDuration();
        }
        v3.set(gold, watchNumPV, followerNum, duration);
        context.write(key, v3);
    }
}
