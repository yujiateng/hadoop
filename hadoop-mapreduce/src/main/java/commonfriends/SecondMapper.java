package commonfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;


public class SecondMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] elems=line.split("\t");
        String people=elems[0];
        String[] friends=elems[1].split(",");
        Arrays.sort(friends);
        for(int i=0;i<friends.length-1;i++){
            for(int j=i+1;j<friends.length;j++){
                context.write(new Text(friends[i]+"-"+friends[j]),new Text(people));
            }
        }

    }
}
