package washweblog;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 清洗日志
 * @author yujiateng
 */
public class WeblogPreProcess {

    static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            WebLogBean webLogBean = WebLogParser.parser(line);
            if (!webLogBean.isValid())
                return;
            k.set(webLogBean.toString());
            context.write(k, NullWritable.get());

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        String input = "data/weblog/input/web.log";
        String output = "data/weblog/output";


        //如果输出路径存在，删除
        tool.Utils.delFolder(conf,output);

        job.setJarByClass(WeblogPreProcess.class);

        job.setMapperClass(WeblogPreProcessMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : -1);

    }

}
