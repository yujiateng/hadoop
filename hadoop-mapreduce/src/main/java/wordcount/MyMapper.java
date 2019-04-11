package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * KEYIN:默认情况下，是mr框架所读到的一行文本的起始偏移量 Long，但是在hadoop中有自己的更精简的序列化接口，所以不直接用Long 而用LongWritable
 * VALUEIN:默认情况下，是mr框架所读到的一行文本的内容 String 同上，用Text
 * KEYOUT:是用户自定义逻辑处理完之后输出数据中的key，在此处是单词 String 同上，用Text
 * VALUEOT:是用户自定义逻辑处理完之后输出数据中的value，在此处是单词数 Integer 同上，用IntWritable
 */
public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer words = new StringTokenizer(line, " ");
        while (words.hasMoreElements())
            context.write(new Text(words.nextToken()), new IntWritable(1));
    }
}
