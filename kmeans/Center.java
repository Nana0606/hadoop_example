import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/*
 * Center类主要是存放质心的个数k，还有两个从hdfs上读取质心的方法。
 * 一个用来读取初始的质心，另一个用来读取每次迭代之后的质心文件夹。
 */
public class Center {
	
	protected static int k = 2; //质心的个数
	
	/*
	 * 从初始的质心文件夹中加载质心，并返回字符串，质心之间用tab分割
	 */
	public String loadInitCenter(Path path) throws IOException{
		StringBuffer sBuffer = new StringBuffer();
		
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		FileSystem hdfs = FileSystem.get(configuration);
		FSDataInputStream dis = hdfs.open(path);
		LineReader in = new LineReader(dis, configuration);
		Text line = new Text();
		System.out.println("Center.java文件中：" + line);
		while(in.readLine(line) > 0){
			sBuffer.append(line.toString().trim());
			//其中，\t是tab的转义字符
			sBuffer.append("\t");
		}
		return sBuffer.toString().trim();
	}
	
	
	/*
	 * 从每次迭代的质心文件中读取质心，并返回字符串
	 */
	public String loadCenter(Path path) throws IOException{
		StringBuffer sBuffer = new StringBuffer();
		
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		FileSystem hdfs = FileSystem.get(configuration);
		FileStatus[] files = hdfs.listStatus(path);
		
		for (int i = 0; i < files.length; i++){
			Path filePath = files[i].getPath();
			if(!filePath.getName().contains("part")) continue;
			FSDataInputStream dis = hdfs.open(filePath);
			LineReader in = new LineReader(dis,configuration);
			Text line = new Text();
			while(in.readLine(line) > 0){
				sBuffer.append(line.toString().trim());
				sBuffer.append("\t");
			}
		}
		return sBuffer.toString().trim();
	}
}
