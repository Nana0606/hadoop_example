package com.itcast.hadoop.muliti;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainJob {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		//MultiOutput是作业名
		Job job = new Job(configuration, "MultiOutput");
		job.setJarByClass(MainJob.class);
		
		job.setMapperClass(MultiOutPutMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MultiOutPutReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
    //输入文件是multioutput.txt
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		MultipleOutputs.addNamedOutput(job, "KeySplit", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "AllPart", TextOutputFormat.class, NullWritable.class, Text.class);

		Path outPath = new Path(args[1]);
		FileSystem fSystem = FileSystem.get(configuration);
		if(fSystem.exists(outPath)){
			fSystem.delete(outPath, true);
		}
		
		FileOutputFormat.setOutputPath(job, outPath);
		job.waitForCompletion(true);
	}

}
