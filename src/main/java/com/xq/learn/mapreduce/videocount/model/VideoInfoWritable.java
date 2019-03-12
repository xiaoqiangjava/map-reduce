package com.xq.learn.mapreduce.videocount.model;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 直播平台视频信息统计
 * 金币数量, 总观看PV, 粉丝关注数量, 视频总开播时长
 * @author xiaoqiang
 * @date 2019/3/11 23:23
 */
public class VideoInfoWritable implements Writable
{
    // 获得的金币
    private Long gold;

    // 观看数量pv
    private Long watchNumPV;

    // 关注量
    private Long followerNum;

    //时长
    private Long duration;

    public void set(Long gold, Long watchNumPV, Long followerNum, Long duration)
    {
        this.gold = gold;
        this.watchNumPV = watchNumPV;
        this.followerNum = followerNum;
        this.duration = duration;
    }

    public Long getGold()
    {
        return gold;
    }

    public Long getWatchNumPV()
    {
        return watchNumPV;
    }

    public Long getFollowerNum()
    {
        return followerNum;
    }

    public Long getDuration()
    {
        return duration;
    }

    /**
     *
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(gold);
        dataOutput.writeLong(watchNumPV);
        dataOutput.writeLong(followerNum);
        dataOutput.writeLong(duration);
    }

    /**
     * 注意: 读取数据的的顺序必须跟write方法中写入的顺序保持一致
     * @param dataInput dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException
    {
        this.gold = dataInput.readLong();
        this.watchNumPV = dataInput.readLong();
        this.followerNum = dataInput.readLong();
        this.duration = dataInput.readLong();
    }

    @Override
    public String toString()
    {
        return "VideoInfoWritable{" +
                "gold=" + gold +
                ", watchNumPV=" + watchNumPV +
                ", followerNum=" + followerNum +
                ", duration=" + duration +
                '}';
    }
}
