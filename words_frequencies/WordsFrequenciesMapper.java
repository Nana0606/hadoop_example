import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordsFrequenciesMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		
		if (line.length() == 0)
		{
			return;
		}
		
		String[] wordStrings = line.split("\\s+");
		for (String word : wordStrings) {			
			context.write(new Text(word.toLowerCase()), new LongWritable(1));			
		}
	} 

}
