package com.oserp.wordsfrequencies;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordsFrequenciesPartitioner extends Partitioner<Text, LongWritable> {

	@Override
	public int getPartition(Text key, LongWritable value, int numOfReducer) {
		 
		// 本例设置reducer个数为25,所以比如长度为26的单词会和长度为1的单词分配到同一个分区
		return key.toString().length() % numOfReducer;
    
	}

}
