/**
 * Created by timger on 8/15/14.
 */
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



public class TimgerRuner {
    public static void main(String[] args) throws IOException {

        System.out.println("init ............");
        JobConf conf = new JobConf(TimgerRuner.class);
        conf.setJobName("timger");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        System.out.println("Start ............");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(TimgerMaper.class);
        conf.setReducerClass(TimgerReducer.class);

        // KeyValueTextInputFormat treats each line as an input record,
        // and splits the line by the tab character to separate it into key and value
        conf.setInputFormat(KeyValueTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        Path InputPath =  new Path(args[0]);
        Path OutPath = new Path(args[1]);

        System.out.println(InputPath.toString());
        System.out.println(OutPath.toString());
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
