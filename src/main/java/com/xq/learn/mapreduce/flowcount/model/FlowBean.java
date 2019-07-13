package com.xq.learn.mapreduce.flowcount.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author xiaoqiang
 * @date 2019/7/12 1:07
 */
public class FlowBean implements Writable
{
    private long upload;

    private long download;

    private long sumFlow;

    /**
     * 提供默认构造参数，只有一个构造参数时可以省略
     */
    public FlowBean()
    {
        super();
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeLong(upload);
        out.writeLong(download);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.upload = in.readLong();
        this.download = in.readLong();
        this.sumFlow = in.readLong();
    }

    public void set(long upload, long download)
    {
        this.upload = upload;
        this.download = download;
        this.sumFlow = upload + download;
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

    public long getSumFlow()
    {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow)
    {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString()
    {
        return "FlowBean{" +
                "upload=" + upload +
                ", download=" + download +
                ", sumFlow=" + sumFlow +
                '}';
    }
}
