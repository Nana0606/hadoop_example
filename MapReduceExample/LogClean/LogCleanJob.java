package log.clean;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogCleanJob extends Configured implements Tool{

	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		try {
			int res = ToolRunner.run(configuration, new LogCleanJob(), args);
			System.exit(res);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration configuration = new Configuration();
		//configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		final Job job = Job.getInstance(configuration, LogCleanJob.class.getSimpleName());
		//设置为可以打包运行
		job.setJarByClass(LogCleanJob.class);
		FileInputFormat.setInputPaths(job, args[0]);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//清理已存在的输出文件
		FileSystem fSystem = FileSystem.get(new URI(args[0]), getConf());
		Path outPath = new Path(args[1]);
		if(fSystem.exists(outPath)){
			fSystem.delete(outPath, true);
		}
		
		boolean success = job.waitForCompletion(true);
		if(success){
			System.out.println("Clean process success!");
		}else{
			System.out.println("Clean process failed!");
		}
		return 0;
	}



}
