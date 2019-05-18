import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class FindFileOnHDFS {

	public static void main(String[] args) throws IOException {

		getHDFSNodes();
		getFileLocal();
		
	}

	public static void getHDFSNodes() throws IOException {

		// HDFS集群节点数
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.163.131:9000");
		FileSystem fs = FileSystem.get(conf);

		// 获取分布式文件系统
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		// 获取所有的节点
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		// 循环打印
		for (int i = 0; i < dataNodeStats.length; i++) {
			System.out.println("DataNode_" + i + "_Name:" + dataNodeStats[i].getHostName());
		}
	}

	// 查询某个文件在HDFS集群的位置
	public static void getFileLocal() throws IOException{
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.163.131:9000");
		FileSystem hdfs = FileSystem.get(conf);
		
		//获取HDFS中/user/hadoop/20170722/word.txt文件信息
		FileStatus fileStatus = hdfs.getFileStatus(new Path("/user/hadoop/20170722/word.txt"));
		//获取文件的块信息
		BlockLocation[] blkLocations = hdfs.getFileBlockLocations(fileStatus,  0, fileStatus.getLen());
	    //循环打印
	    int blockLen = blkLocations.length;
		for(int i = 0; i < blockLen; i++){
			String[] hosts = blkLocations[i].getHosts();
			System.out.println("block_" + i + "_locations:" + hosts[0]);
		}
	}

}
