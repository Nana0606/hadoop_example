import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountHbaseMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private Text word = new Text();

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		StringTokenizer iTokenizer = new StringTokenizer(value.toString());
		while(iTokenizer.hasMoreTokens()){
			word.set(iTokenizer.nextToken());
			context.write(word, new IntWritable(1));
		}
	}	

}
