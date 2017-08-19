import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyToHDFS {
	
	public static void main(String[] args) throws IOException {
		
		
		//copyFromLocal，主要操作是将本地文件复制到HDFS
		 
		//step1: 获取Configuration对象
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.163.131:9000");
		//step2: 获取FileSystem对象
		FileSystem  fs = FileSystem.get(conf);
		
    //源文件存放在f盘下，文件名为word.txt
		Path source = new Path("f:" + File.separator + "word.txt");
    //生成的目标文件放在hdfs的/user/hadoop/20170722目录下，文件名为word.txt
		Path dst = new Path("/user/hadoop/20170722/word.txt");
		
		//step3	
		fs.copyFromLocalFile(source, dst);
	}

}
 
