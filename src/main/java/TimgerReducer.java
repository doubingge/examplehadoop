/**
 * Created by timger on 8/15/14.
 */
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class TimgerReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Set<String> attackers = new TreeSet<String>();
        while (values.hasNext()) {
            String valStr = values.next().toString();
            attackers.add(valStr);
        }
        attackers.
        output.collect(key, new Text(attackers.toString()));
    }
}



