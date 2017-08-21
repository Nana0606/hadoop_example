package com.hbase.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/*
 * WordCount示例：
 * 统计字数并存入HBase中
 */
public class WordCountHbaseTest {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		TableName tablename = TableName.valueOf("wordcount");
		Configuration configuration = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(configuration);
		Admin admin = connection.getAdmin();
		if(admin.tableExists(tablename)){
			System.out.println("table exists! recreate it....");
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
		}
		HTableDescriptor htd = new HTableDescriptor(tablename);
		HColumnDescriptor hcd = new HColumnDescriptor("content");
		htd.addFamily(hcd); //创建列族
		admin.createTable(htd); //创建表
		
		Job job = new Job(configuration, "WordCountHbase");
		job.setJarByClass(WordCountHbaseTest.class);
		
		//使用WordCountHbaseMapper类实现Map过程
		job.setMapperClass(WordCountHbaseMapper.class);
		TableMapReduceUtil.initTableReducerJob(tablename.toString(), WordCountHbaseReducer.class, job);
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.163.131:9000/input/sort.txt"));
		//设置map过程和reduce过程的输出类型，其中设置key的输出类型是Text,value的输出类型为IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//调用job.waitForCompletion(true)执行任务，执行成功后退出
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
