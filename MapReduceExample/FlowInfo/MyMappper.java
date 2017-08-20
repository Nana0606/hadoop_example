package org.apache.hadoop.mapreduce.app;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, DataWritable> {
	
	DataWritable data = new DataWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//System.out.println("调用了map函数");
		String line = value.toString();
		System.out.println(line);
		String[] strs = line.split("\t");
		Text mobile = new Text(strs[0].getBytes());
		int upPackNum = Integer.parseInt(strs[6]);
		int upPayLoad = Integer.parseInt(strs[7]);
		int downPackNum = Integer.parseInt(strs[8]);
		int downPayLoad = Integer.parseInt(strs[9]);
		data.set(upPackNum, upPayLoad, downPackNum, downPayLoad);
		context.write(mobile, data);
		//System.out.println("map函数中写入成功");
	}

	
	
}
