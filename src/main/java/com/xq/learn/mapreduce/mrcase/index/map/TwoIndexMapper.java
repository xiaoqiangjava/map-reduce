package com.xq.learn.mapreduce.mrcase.index.map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Hello-->b.txt	3
 * Hello-->bbb.txt	3
 * 封装成
 * Hello    b.txt-->3
 * Hello    bbb.txt-->3
 * 使用KeyValueInputFormat读取文件内容
 * @author xiaoqiang
 * @date 2019/7/21 14:09
 */
public class TwoIndexMapper extends Mapper<Text, Text, Text, Text>
{
    private static final String KEY_SEP = "-->";

    private Text keyOut = new Text();

    private Text valOut = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException
    {
        // 获取文件内容
        String line = key.toString();
        String[] fields = line.split(KEY_SEP);
        keyOut.set(fields[0]);
        valOut.set(fields[1] + KEY_SEP + value.toString());
        context.write(keyOut, valOut);
    }
}
