import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, Text, Text, Text> {
	
    public void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      while (values.hasNext()) {
          context.write(key, new Text(values.next().getBytes()));
      }               
    }
  }

