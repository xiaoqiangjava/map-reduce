package com.xq.learn.hbase;

import java.util.Map;

/**
 * main
 * @author xiaoqiang
 * @date 2019/3/14 21:08
 */
public class Main
{
    private static final String COLUMN_FAMILY = "cf1";

    public static void main(String[] args)
    {
        HBaseOperation operation = new HBaseOperation("t1");
        String rowKey = "wenwen";
        String qualifier = "grade";
        String value = "S+";
        boolean success = operation.put(rowKey, COLUMN_FAMILY, qualifier, value);
        if (success)
        {
            System.out.println("插入数据成功!");
        }
        String result = operation.getValue(rowKey, COLUMN_FAMILY, qualifier);
        System.out.println("查询数据: " + result);
        Map<String, String> values = operation.getValues(COLUMN_FAMILY, qualifier);
        for (String info : values.keySet())
        {
            System.out.println("<" + info + "," + values.get(info) +">");
        }
    }
}
