package tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Utils {

    public Utils() {
    }

    public static void delFolder(Configuration conf, String... paths) throws IOException {
        FileSystem fs = FileSystem.newInstance(conf);
        for (String str : paths) {
            Path path = new Path(str);
            if (fs.exists(path))
                fs.delete(path, true);
        }
    }
}
