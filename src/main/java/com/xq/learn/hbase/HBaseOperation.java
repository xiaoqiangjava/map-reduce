package com.xq.learn.hbase;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 使用Java操作HBase
 * 表的操作DDL: 主要使用admin来操作
 * 数据的操作DML: 主要使用Table来操作
 * @author xiaoqiang
 * @date 2019/3/13 23:22
 */
public class HBaseOperation
{
    private static final Logger logger = LoggerFactory.getLogger(HBaseOperation.class);
    private Configuration conf;

    private Connection conn;

    private Table table;

    public HBaseOperation(String tableName)
    {
        init(tableName);
    }

    /**
     * 初始化
     */
    public void init(String tableName)
    {
        // 1. 设置环境变量
        System.setProperty("hadoop.home.dir", "D:\\Learn\\Workspace\\BigData\\MapReduce\\mapreduce-demo\\lib");
        // 2. 根据配置文件创建conf
        conf = HBaseConfiguration.create();
        try
        {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
        }
        catch (IOException e)
        {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 关闭连接
     */
    public void close()
    {
        IOUtils.closeQuietly(conn);
    }

    /**
     * 表中插入记录
     * @param tableName 表名称
     * @param rowKey rowKey
     * @param columnFamily columnFamily
     * @param qualifier qualifier
     * @param value value
     * @return boolean
     */
    public boolean put(String rowKey, String columnFamily, String qualifier, String value)
    {
        try
        {
            // 2. 创建Put对象, 封装rowKey, cf, qualifier, value
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            // 3. 将值put到表中
            table.put(put);
            return true;
        }
        catch (IOException e)
        {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    /**
     * 获取一条记录
     * @param rowKey rowKey
     * @param columnFamily columnFamily
     * @param qualifier qualifier
     * @return value
     */
    public String getValue(String rowKey, String columnFamily, String qualifier)
    {
        try
        {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            Result result = table.get(get);
            byte[] value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            return Bytes.toString(value);
        }
        catch (IOException e)
        {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * 扫描 values
     * @param columnFamily columnFamily
     * @param qualifier qualifier
     * @return values
     */
    public Map<String, String> getValues(String columnFamily, String qualifier)
    {
        Map<String, String> values = new HashMap<>();
        try
        {
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext())
            {
                Result result = iterator.next();
                String rowKey = Bytes.toString(result.getRow());
                String value = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier)));
                values.put(rowKey, value);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return values;
    }
}
