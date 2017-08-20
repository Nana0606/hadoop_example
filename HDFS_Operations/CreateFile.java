import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CreateFile {
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://192.168.163.131:9000");
		byte[] buff = "hello world!".getBytes();
		FileSystem hdfs = FileSystem.get(conf);
		Path dfs = new Path("/test");
		FSDataOutputStream outputStream = hdfs.create(dfs);
		outputStream.write(buff, 0, buff.length);
		}

}
