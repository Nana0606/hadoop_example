package com.itcast.hadoop.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
*主要功能：
* 将
str1	2
str2	5
str1	5
str1	3
str2	7
str2	1
输出成：
str1	2,3,5
str2	1,5,7
*/
public class SortMain {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: wordcount <in><out>");
			System.exit(2);
		}
		Job job = new Job(configuration, "Sort");
		job.setJarByClass(SortMain.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setMapOutputKeyClass(IntPaire.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(TextIntComparator.class);
		
		job.setGroupingComparatorClass(TextComparator.class);
		
		job.setPartitionerClass(PartitionByText.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
