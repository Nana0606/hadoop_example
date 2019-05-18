import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSMkdir {
	
	public static void main(String[] args) throws IOException{
		
		
		//step1
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://192.168.163.131:9000");
		
		//step2
		FileSystem fs = FileSystem.get(conf);
		
		//step3
		fs.mkdirs(new Path("/user/hadoop/20120722"));
		
		//step4
		FileStatus fileStatus = fs.getFileStatus(new Path("/user/hadoop/20120722"));
		System.out.println(fileStatus.getPath());
	}

}
