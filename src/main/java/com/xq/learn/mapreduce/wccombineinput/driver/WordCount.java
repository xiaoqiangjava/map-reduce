package com.xq.learn.mapreduce.wccombineinput.driver;

import com.xq.learn.mapreduce.wccombineinput.map.WcMapper;
import com.xq.learn.mapreduce.wccombineinput.reduce.WCReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 相当于Yarn客户端，负责提交MapReduce任务
 * 使用CombineFileInputFormat多个小文件做了优化，使多个小文件切分到一个切片，减少map task的数量
 * @author xiaoqiang
 * @date 2019/7/11 2:02
 */
public class WordCount
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        // 1. 获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, WordCount.class.getSimpleName());
        // 2. 指定jar包的路径
        job.setJarByClass(WordCount.class);

        // 3. 关联Mapper和Reducer类
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WCReducer.class);

        // 4. 指定Mapper输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 指定最终的输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. 指定输出输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 设置文件输入类，合并多个小文件, 需要设置相应的实现类，如果是抽象类，报错：InstantiationException
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 8. 提交作业
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
