package org.apache.hadoop.mapreduce.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Run {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//get conf
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		
		//create job
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(Run.class);
		
		// 1) input
		//job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/input/flowInfo.txt"));
		
		// 2) map
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataWritable.class);
		
		// 3) reduce
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataWritable.class);
		
		// 4) output
		FileOutputFormat.setOutputPath(job, new Path("/output/flowOut"));
		
		// 5) submit and return status
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
