import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text; 

public class WordsFrequenciesCombiner extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable,Text,LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		long counts = 0;

		System.out.printf("\r[Combiner的输入键值] " + "Key:%s\t\t\tValue:",key);
		for (LongWritable value : values) {
			counts += value.get();	
			System.out.printf("%s  ",value.get());
		}
		context.write(key, new LongWritable(counts));		
	}
}
