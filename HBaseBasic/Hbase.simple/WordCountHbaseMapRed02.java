package com.hbase.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 来自：http://www.2cto.com/net/201605/510155.html
 * Mapper负责写到HDFS上
 * Reducer负责写到HBase上
 */
public class WordCountHbaseMapRed02 {
	
	public static class MyHbaseMap02 extends TableMapper<Text, Text>{
		
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			run();
		}

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String word = null;
			String num = null;
			List<Cell> cs = value.listCells();
			for(Cell cell:cs){
				word = Bytes.toString(CellUtil.cloneRow(cell));
				num = Bytes.toString(CellUtil.cloneValue(cell));
			}
			context.write(new Text(word), new Text(num));
		}
		
		public static void run() throws IOException, ClassNotFoundException, InterruptedException{
			Configuration configuration = new Configuration();
			configuration = HBaseConfiguration.create(configuration);
			
			Job job = Job.getInstance(configuration, "wordcount2");
			job.setJarByClass(WordCountHbaseMapRed02.class);
			
			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("wordcount"), Bytes.toBytes("num"));
			
			//数据来源hbase
			TableMapReduceUtil.initTableMapperJob("word", scan, MyHbaseMap02.class, Text.class, Text.class, job);
			FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.163.131:9000/output/hbaseOut"));
			
			System.exit(job.waitForCompletion(true)?0:1);
		}
		
		
		
	}

}
