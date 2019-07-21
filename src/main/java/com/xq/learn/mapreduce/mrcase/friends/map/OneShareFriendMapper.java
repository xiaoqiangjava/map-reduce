package com.xq.learn.mapreduce.mrcase.friends.map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入：好友列表
 * A:B,C,D,F,E,O
 * B:A,C,E,K
 * C:B,K
 * 输出：B，C，D，E，F都是谁的好友
 * B    A
 * B    C
 * @author xiaoqiang
 * @date 2019/7/21 15:20
 */
public class OneShareFriendMapper extends Mapper<Text, Text, Text, Text>
{
    private Text keyOut = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException
    {
        // 获取数据
        String line = value.toString();
        String[] friends = line.split(",");
        // 输出每个人都是谁的好友
        for (String friend : friends)
        {
            keyOut.set(friend);
            context.write(keyOut, key);
        }
    }
}
