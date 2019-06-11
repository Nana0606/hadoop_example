import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordsFrequenciesRunner extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordsFrequenciesRunner(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.printf("Args missing. Input path and output path is required.");
			return -1;
		}

		Configuration conf = getConf();
		
		// 设置数据文件分割大小。设置的数值越小，所需读mapper就越多。
		// 比如此例，400KB的数据文件，设置分割标准为20K，则最终大概会需要20个mapper。
		// 此例中用20多个mapper来处理一个400KB的数据文件，会比用一个mapper花的时间长很多。
		//conf.setStrings("mapred.max.split.size", "20000");
		
		Job job = Job.getInstance(conf);
		
		// 设置reducer读个数。每个reducer最终会产生一个输出文件
		job.setNumReduceTasks(3);
		
		// 自定义分区类
		// 本例中，长度相同的单词会被同一个reducer处理，最终也会出现在同一个输出文件中		
		job.setPartitionerClass(WordsFrequenciesPartitioner.class);
		
		job.setJobName("Calculate words frequencies");
		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.setMapperClass(WordsFrequenciesMapper.class);
		job.setReducerClass(WordsFrequenciesReducer.class);
		job.setCombinerClass(WordsFrequenciesCombiner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
