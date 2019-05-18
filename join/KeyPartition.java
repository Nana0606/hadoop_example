package com.itcast.hadoop.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartition extends Partitioner<TextPair, Text>{

	@Override
	public int getPartition(TextPair key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		System.out.println("进入keypartition");
		return (key.getText().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
