package com.xq.learn.mapreduce.mrcase.friends.map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * A	I,K,C,B,G,F,H,O,D,
 * B	A,F,J,E,
 * 将value两两组合
 * @author xiaoqiang
 * @date 2019/7/21 15:44
 */
public class TwoShareFriendMapper extends Mapper<Text, Text, Text, Text>
{
    private Text valOut = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] persons = value.toString().split(",");
        Arrays.sort(persons);
        for (int i = 0; i < persons.length -1; i++)
        {
            for (int j = i + 1; j < persons.length; j++)
            {
                // 将人-人两两组合写出，这样相同的人-人就会到同一个reduce方法
                String person2person = persons[i] + "-" + persons[j];
                valOut.set(person2person);
                context.write(valOut, key);
            }
        }
    }
}
