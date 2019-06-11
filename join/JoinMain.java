import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/*这个代码有问题，得出的答案是：
 * trade3	pay3
 * pay4	pay2
 * pay4	trade2
 * trade1	pay1
 * 不符合要求
 * 
 * */

public class JoinMain {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if(otherArgs.length != 3){
			System.err.println("Usage: <tradeTableDir><payTableDir><ouput>");
			System.exit(2);
		}
		String tradeTableDir = args[0];
		String payTableDir = args[1];
		String joinTableDir = args[2];
		Job job = new Job(configuration, "Join");
		job.setJarByClass(JoinMain.class);
		job.setMapperClass(PreMapper.class);	
		job.setReducerClass(CommonReduce.class);
		
		
		job.setMapOutputKeyClass(TextPair.class);
		//job.setMapOutputValueClass(Text.class);
		
		job.setGroupingComparatorClass(FirstComparator.class);
		
		job.setPartitionerClass(KeyPartition.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		FileInputFormat.addInputPath(job, new Path(tradeTableDir));
		FileInputFormat.addInputPath(job, new Path(payTableDir));
		FileOutputFormat.setOutputPath(job, new Path(joinTableDir));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
