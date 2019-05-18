package com.hbase.wordcount;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WordCountHbaseReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		for(IntWritable value: values){ //遍历求和
			sum += value.get();	
		}
		Put put = new Put(key.getBytes()); //put实例化，每一个词存一行
		//列族为content，列修饰符为count，列值为数目
		put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
		context.write(new ImmutableBytesWritable(key.getBytes()), put);
	}
	
	

}
