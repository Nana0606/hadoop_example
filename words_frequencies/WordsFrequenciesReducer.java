import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordsFrequenciesReducer extends Reducer<Text, LongWritable, Text,LongWritable > {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		long counts = 0;

		for (LongWritable value : values) {
			counts += value.get();	
			System.out.printf("\r[Reduce函数的输入键值对]" + " key:%s\t\t\tValue:%s",key,value.get());
		}
		
		context.write(key, new LongWritable(counts));
	}

}
