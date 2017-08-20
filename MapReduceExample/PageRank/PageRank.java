package com.pagerank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

	public static class Map extends Mapper<Object,Text,IntWritable,FloatWritable>{
		private final IntWritable word = new IntWritable();
		private String pr;
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			if(itr.hasMoreTokens())   {
				String id = itr.nextToken();
			}else 
				return;
			pr = itr.nextToken();          //网页的pr值
			int count = itr.countTokens();     //链接ID的数目
			float average_pr = Float.parseFloat(pr)/count;
			while(itr.hasMoreTokens()){
				word.set(Integer.parseInt(itr.nextToken()));
				context.write(word, new FloatWritable(average_pr));
			}
		}
	}
	
	public static class Reduce extends Reducer<IntWritable,FloatWritable,IntWritable,FloatWritable>{
		float sum;
		public void reduce(IntWritable key,Iterable<FloatWritable>values,Context context) throws IOException, InterruptedException{
			for(FloatWritable val:values){
				sum += val.get();
			}
			context.write(key,new FloatWritable(sum));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		Job job = new Job(configuration);
		job.setJarByClass(PageRank.class);
		job.setNumReduceTasks(3);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/input/pagerank.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/output/pagerankout"));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

}
