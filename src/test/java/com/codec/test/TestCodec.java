package com.codec.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author xiaoqiang
 * @date 2019/7/19 1:00
 */
public class TestCodec
{
    /**
     * 压缩文件
     * @throws ClassNotFoundException ClassNotFoundException
     * @throws IOException IOException
     */
    @Test
    public void testDefaultCodec() throws ClassNotFoundException, IOException
    {
        String name = "D:\\software dev\\VMware\\VMware-workstation-full-15.0.0-10134415.exe";
        InputStream inputStream = new FileInputStream(name);
        Class<?> clazz = Class.forName("org.apache.hadoop.io.compress.DefaultCodec");
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clazz, conf);
        CompressionOutputStream outputStream = codec.createOutputStream(new FileOutputStream(name + codec.getDefaultExtension()));
        IOUtils.copyBytes(inputStream, outputStream, conf);
    }

    /**
     * 解压缩
     * @throws ClassNotFoundException ClassNotFoundException
     * @throws IOException IOException
     */
    @Test
    public void testDefaultDecodec() throws ClassNotFoundException, IOException
    {
        String name = "D:\\software dev\\VMware\\VMware-workstation-full-15.0.0-10134415.exe.deflate";
        Class<?> clazz = Class.forName("org.apache.hadoop.io.compress.DefaultCodec");
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clazz, conf);
        CompressionInputStream inputStream = codec.createInputStream(new FileInputStream(name));
        OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(name + ".bak"));
        IOUtils.copyBytes(inputStream, outputStream, conf);
    }
}
