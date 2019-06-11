import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

/*
 * Hbase -- > Hbase 
 * 将hbase中存储的文件复制到hbase中
 */
public class WordCountHbaseMapRed03 {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		run();
	}
	
	public static class MyHBaseMap03 extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable>{

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				 Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ImmutableBytesWritable word = null;
			ImmutableBytesWritable num = null;
			List<Cell> cs = value.listCells();
			for(Cell cell:cs){
				word = new ImmutableBytesWritable(CellUtil.cloneRow(cell));
				num = new ImmutableBytesWritable(CellUtil.cloneValue(cell));
			}
			context.write(word, num);
		}
		
	}

	private static void run() throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration configuration = new Configuration();
		configuration = HBaseConfiguration.create(configuration);
		
		Job job = Job.getInstance(configuration, "wordcount3");
		job.setJarByClass(WordCountHbaseMapRed03.class);
		
		Scan scan = new Scan();
		
		scan.addColumn(Bytes.toBytes("wordcount"), Bytes.toBytes("num"));
		
		//数据来源hbase
		TableMapReduceUtil.initTableMapperJob("word", scan, MyHBaseMap03.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
	    createTable(configuration);
	    
	    TableMapReduceUtil.initTableReducerJob("word2", MyHBaseReducer03.class, job);
	    
	    System.exit(job.waitForCompletion(true)?0:1);
	}

	private static void createTable(Configuration configuration) throws IOException {
		// TODO Auto-generated method stub
		Connection connection = ConnectionFactory.createConnection(configuration);
		Admin admin = connection.getAdmin();
		TableName tableName = TableName.valueOf("word2");
		if(!admin.tableExists(tableName)){
			HTableDescriptor htd = new HTableDescriptor(tableName);
			HColumnDescriptor hcd = new HColumnDescriptor("wordcount");
			htd.addFamily(hcd);
			admin.createTable(htd);
			System.out.println("表不存在，新创建表成功....");
		}
	}
	
	public static class MyHBaseReducer03 extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable>{

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Put put = new Put(key.get());
			put.addColumn(Bytes.toBytes("wordcount"), Bytes.toBytes("num"), values.iterator().next().get());
			context.write(new ImmutableBytesWritable(key.get()), put);
		}
		
	}
	
}
