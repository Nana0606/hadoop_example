import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * Eclipse连接HBase的基本操作
 */
public class HbaseBean {

	static Configuration configuration = HBaseConfiguration.create();
	public static Connection connection;
	public static Admin admin;

	// create a new table.
	//columnFamily代表列族
	public static void create(TableName tablename, String columnFamily) throws Exception {
		connection = ConnectionFactory.createConnection(configuration);
		admin = connection.getAdmin();
		if (admin.tableExists(tablename)) {
			System.out.println("table Exists!");
			System.exit(0);
		} else {
			//can use HTableDescriptor and HColumnDescriptor to modify table pattern.
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table successfully!");
		}
	}

	// insert a record.
	public static void put(TableName tablename, String row, String columnFamily, String column, String data)
			throws Exception {
		connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(tablename);
		Put p = new Put(Bytes.toBytes(row));
		p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
		table.put(p);
		System.out.println("put '" + row + "','" + columnFamily + ":" + column + "','" + data + "'");
	}

	// get data of some row for one table.
	// which equals to hbase shell command of " get 'tablename','rowname' "
	public static void get(TableName tablename, String row) throws IOException {
		connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(tablename);
		Get g = new Get(Bytes.toBytes(row));
		Result result = table.get(g);
		System.out.println("Get Info: " + result);
	}

	// get all data of this table, using "Scan" to operate.
	public static void scan(TableName tablename) throws Exception {
		connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(tablename);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		for (Result r : rs) {
			System.out.println("Scan info: " + r);
		}
	}

	// delete a table, this operation needs to disable table firstly and then delete it.
	public static boolean delete(TableName tablename) throws IOException {
		connection = ConnectionFactory.createConnection(configuration);
		admin = connection.getAdmin();
		if (admin.tableExists(tablename)) {
			try {
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
			} catch (Exception ex) {
				ex.printStackTrace();
				return false;
			}

		}
		return true;
	}

	public static void main(String[] agrs) {
		TableName tablename = TableName.valueOf("hbase_test");
		String columnFamily = "columnVal";

		try {
            //Step1: create a new table named "hbase_test".
			HbaseBean.create(tablename, columnFamily);
			//Step2: insert 3 records.
			HbaseBean.put(tablename, "row1", columnFamily, "1", "value1");
			HbaseBean.put(tablename, "row2", columnFamily, "2", "value2");
			HbaseBean.put(tablename, "row3", columnFamily, "3", "value3");
			//Step3： get value of row1.
			HbaseBean.get(tablename, "row1");
			//Step4: scan the full table.
			HbaseBean.scan(tablename);
			//Step4: delete this table.
			if (HbaseBean.delete(tablename) == true)
				System.out.println("Delete table:" + tablename + " success!");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
