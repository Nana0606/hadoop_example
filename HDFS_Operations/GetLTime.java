import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GetLTime {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("df.default.name", "hdfs://192.168.163.131:9000");
		FileSystem hdfs = FileSystem.get(conf);
		Path fpath = new Path("/test.txt");
		FileStatus fileStatus = hdfs.getFileStatus(fpath);
		long modificationTime = fileStatus.getModificationTime();
		System.out.println("Modification time is:" + modificationTime);
	}

}
