package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * <hello,1><hello,1><hello,1><hello,1><hello,1><hello,1><hello,1><hello,1>...
 * <angelababy,1> <angelababy,1> <angelababy,1><angelababy,1><angelababy,1>...
 * key是一组相同单词的<key,value>对的key
 * values是一组相同单词的<key,value>对的value
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(IntWritable value:values){
            count+=value.get();
        }
        context.write(new Text(key),new IntWritable(count));
    }
}

