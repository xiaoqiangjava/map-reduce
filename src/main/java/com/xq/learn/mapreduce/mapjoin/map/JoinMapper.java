package com.xq.learn.mapreduce.mapjoin.map;

import com.xq.learn.mapreduce.reducejoin.model.OrderBean;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * 在map端实现join，前提条件是有一个数据量比较小的表，提前将这张表的数据加载到内存中，分装为map
 * 需要设置分布式缓存，将小表文件散发到各个map task
 * @author xiaoqiang
 * @date 2019/7/17 1:46
 */
public class JoinMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>
{
    private BufferedReader reader;

    private Map<Integer, String> cache = new HashMap<>();

    private OrderBean outKey = new OrderBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//        Path[] files = new Path[]{new Path("D:/data/input/order/product.txt")};
        for (Path path : files)
        {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toString()), Charset.forName("UTF-8")));
            String line;
            while ((line = reader.readLine()) != null)
            {
                String[] fields = line.split("\t");
                cache.put(Integer.parseInt(fields[0]), fields[1]);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] fields = line.split("\t");
        outKey.setOrderId(fields[0]);
        outKey.setPrice(Double.parseDouble(fields[2]));
        int pid = Integer.parseInt(fields[1]);
        outKey.setpId(pid);
        outKey.setpName(cache.get(pid));
        context.write(outKey, NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException
    {
        reader.close();
    }


}
