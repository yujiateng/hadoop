package commonfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FirstReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text friend, Iterable<Text> peoples, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer=new StringBuffer();
        for(Text people:peoples){
            stringBuffer.append(people).append(",");
        }

        String result=stringBuffer.substring(0,stringBuffer.length()-1);
        context.write(new Text(friend),new Text(result));
    }
}
