import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * 功能：在向HDFS上传复制文件的过程中，进行合并文件
 */
public class PutMerge {
	
	/*
	 * 复制上传文件，并将文件合并
	 * 
	 * @param localDir 本地要上传的文件目录
	 * @param hdfsFile HDFS上的文件名称，包括路径 
	 * 
	 * 主要思路：
	 * Step1：打开输入流，读取本地文件
	 * Step2：打开输出流，写入HDFS文件
	 * Step3：循环遍历本地文件
	 */
	public static void put(String localDir, String hdfsFile){
		
		//Step1:获取配置信息
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://192.168.163.131:9000");
		
		//路径
		Path localPath = new Path(localDir);
		Path hdfsPath = new Path(hdfsFile);
		
		try {
			//获取本地文件系统
			FileSystem localFs = FileSystem.getLocal(conf);
			//获取HDFS文件系统
			FileSystem hdfs = FileSystem.get(conf);
			
			//获取本地文件系统指定目录中的所有文件
			FileStatus[] status = localFs.listStatus(localPath);
			
			
			//打开HDFS文件的输出流
			FSDataOutputStream fsDataOutputStream = hdfs.create(hdfsPath);
			
			//循环遍历本地文件
			for (FileStatus fileStatus: status){
				//获取文件
				Path path = fileStatus.getPath();
				System.out.println("文件为：" + path.getName());
				
				//打开文件输入流
				FSDataInputStream fsDataInputStream = localFs.open(path);
			
			    //进行流的读写操作
				byte[] buffer = new byte[1024];
				int len = 0;
				while((len = fsDataInputStream.read(buffer)) > 0){
					fsDataOutputStream.write(buffer, 0, len);
				}
				fsDataInputStream.close();
			}
			
			fsDataOutputStream.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		
		String localPath = "F:/code/eclipse/input";
		String hdfsPath = "/tmp/inputs_Merge";
		put(localPath, hdfsPath);
		
	}

}
