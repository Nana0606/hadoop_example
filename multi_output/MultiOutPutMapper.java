package com.itcast.hadoop.muliti;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultiOutPutMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

	//将每行的“,”之前的元素作为key，其余值作为value
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
    
		//The function of trim() is to remove spaces in the front and the back of one string. 
		String line = value.toString().trim();
		if(null != line && 0 != line.length()){
			String[] arr = line.split(",");	
			context.write(new IntWritable(Integer.parseInt(arr[0])), value);
			//存储的key-value格式，
			//举例：“1101,thinkpad yoga,联想,windows 8,超级本”将被存储为“1101-1101,thinkpad yoga,联想,windows 8,超级本”
		}
	}
	
	

}
