/**
 * Created by timger on 8/15/14.
 */
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TimgerMaper extends MapReduceBase implements Mapper<Text, Text, Text, Text>{
    public void map(Text attacker, Text victim, OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {
        output.collect(victim, attacker);
    }
}
