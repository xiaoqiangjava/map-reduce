package com.xq.learn.mapreduce.sortkey.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 当自定义的bean作为MapReduce任务的key时，必须实现WritableComparable接口，因为在map结束后会对key进行排序
 * 如果未指定Partitioner，还必须实现hashCode()方法，因为默认使用HashPartitioner来分区
 * @author xiaoqiang
 * @date 2019/7/14 13:03
 */
public class FlowBean implements WritableComparable<FlowBean>
{
    private long upload;

    private long download;

    private long sum;

    public void set(long upload, long download)
    {
        this.upload = upload;
        this.download = download;
        this.sum = this.upload + this.download;
    }

    @Override
    public int compareTo(FlowBean that)
    {
        return Long.compare(this.sum, that.sum);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeLong(upload);
        out.writeLong(download);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.upload = in.readLong();
        this.download = in.readLong();
        this.sum = this.upload + this.download;
    }

    public long getUpload()
    {
        return upload;
    }

    public void setUpload(long upload)
    {
        this.upload = upload;
    }

    public long getDownload()
    {
        return download;
    }

    public void setDownload(long download)
    {
        this.download = download;
    }

    public long getSum()
    {
        return sum;
    }

    public void setSum(long sum)
    {
        this.sum = sum;
    }

    @Override
    public String toString()
    {
        return "FlowBean{" +
                "upload=" + upload +
                ", download=" + download +
                ", sum=" + sum +
                '}';
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(upload, download, sum);
    }
}
