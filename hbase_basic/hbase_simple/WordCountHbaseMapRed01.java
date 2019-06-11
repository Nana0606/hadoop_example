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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

/*
 * HDFS --> HBase
 * 将HDFS中的数据写到HBase上
 */
public class WordCountHbaseMapRed01 {
	
	private static String tablename = "word";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		run();
	}

	private static void run() throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration configuration = new Configuration();
	        configuration = HBaseConfiguration.create(configuration);
		
		Job job = Job.getInstance(configuration, "MyWordCount");
		job.setJarByClass(WordCountHbaseMapRed01.class);
		
		//下面的3行配置必须写，不写的话可以创建表格，没有错误提示，但是写入的数据为空
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//输入文件，part-r-00000是wordcount的输出文件，即单词及其出现的次数
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.163.131:9000/output/wordcountOut/part-r-00000"));
		checkTable(configuration);
		TableMapReduceUtil.initTableReducerJob(tablename, MyHbaseReducer.class, job);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

	private static void checkTable(Configuration configuration) throws IOException {
		// TODO Auto-generated method stub
		Connection connection = ConnectionFactory.createConnection(configuration);
		Admin admin = connection.getAdmin();
		TableName tableName = TableName.valueOf(tablename);
		if(!admin.tableExists(tableName)){
			HTableDescriptor htd = new HTableDescriptor(tableName);
			HColumnDescriptor hcd = new HColumnDescriptor("wordcount");
			htd.addFamily(hcd);
			admin.createTable(htd);
			System.out.println("create table successfully!");
		}
	}
	
	public static class MyHbaseReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Put put = new Put(key.toString().getBytes());
			put.addColumn(Bytes.toBytes("wordcount"), Bytes.toBytes("num"), Bytes.toBytes(String.valueOf(values.iterator().next())));
			context.write(new ImmutableBytesWritable(key.getBytes()), put);
		}
		
	}
	


}
