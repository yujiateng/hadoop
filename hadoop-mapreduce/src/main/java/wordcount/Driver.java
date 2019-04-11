package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 单词统计
 *
 * @author yujiateng
 */
public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);
        job.setJobName("wordcount");

        String input = "data/wordcount/input/words.txt";
        String output = "data/wordcount/output";

        //如果输出路径存在，删除
        tool.Utils.delFolder(conf,output);

        //指定本程序的jar包所在的本地路径
        job.setJarByClass(Driver.class);

        //指定本业务job要使用的mapper和reducer业务类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //指定mapper输出数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终输出数据的<key,value>类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定job输出数据的位置
        FileInputFormat.setInputPaths(job, new Path(input));
        //指定job输出结果所在位置
        FileOutputFormat.setOutputPath(job, new Path(output));

        //将job中配置的相关参数以及job所用的java类所在的jar包提交给yarn
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion ? 0 : -1);
    }
}
