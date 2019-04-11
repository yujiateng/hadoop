package commonfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 找共同好友
 *
 * @author yujiateng
 */
public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        String input1 = "data/friends/input/friends.txt";
        String output1 = "data/friends/input/output";
        String input2 = "data/friends/input/output/part-r-00000";
        String output2 = "data/friends/output";

        //如果输出路径存在，删除
        tool.Utils.delFolder(conf,output1,output2);

        //第一阶段
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(FirstMapper.class);
        job1.setReducerClass(FirstReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(input1));
        FileOutputFormat.setOutputPath(job1, new Path(output1));
        System.out.println(job1.waitForCompletion(true) ? 0 : -1);

        //第二阶段
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(Driver.class);
        job2.setMapperClass(SecondMapper.class);
        job2.setReducerClass(SecondReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(input2));
        FileOutputFormat.setOutputPath(job2, new Path(output2));
        System.out.println(job2.waitForCompletion(true) ? 0 : -1);
    }
}
