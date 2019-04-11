package commonfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 原始数据结构 <people-friends>
 * Map后输出 <friend-people>
 */
public class FirstMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] elems=value.toString().split(":");
        String people=elems[0];
        String[] friends=elems[1].split(",");
        for(String friend:friends){
            context.write(new Text(friend),new Text(people));
        }
    }
}
