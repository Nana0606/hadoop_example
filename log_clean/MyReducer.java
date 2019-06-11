import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<LongWritable, Text, Text, NullWritable>{

	@Override
	protected void reduce(LongWritable k2, Iterable<Text> v2s,
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(Text v2: v2s){
			context.write(v2, NullWritable.get());
		}
	}
	

}
