package commonfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class SecondReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer=new StringBuffer();
        Set<String> set=new HashSet<>();
        for (Text value:values){
            if (!set.contains(value.toString())){
                set.add(value.toString());
            }
        }
        for (String s:set){
            stringBuffer.append(s).append(",");
        }
        String result=stringBuffer.toString().substring(0,stringBuffer.length()-1);
        context.write(key,new Text(result));

    }
}

