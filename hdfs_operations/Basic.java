import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.DistributedFileSystem;



public class Basic {
	
	private static final Log LOG = LogFactory.getLog(Basic.class);
	
	/*
	 * Reads the directory name(s) and file name(s) from the specified parameter "srcPath"
	 */
	public void list(String srcPath){
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name","hdfs://192.168.163.131:9000");
		LOG.info("[Defaults]: " + configuration.get("fs.default.name"));
		FileSystem fs;
		try {
			fs = FileSystem.get(configuration);
			RemoteIterator<LocatedFileStatus> rmIterator = fs.listLocatedStatus(new Path(srcPath));
			while(rmIterator.hasNext()){
				Path path = rmIterator.next().getPath();
				if(fs.isDirectory(path)){
					LOG.info("------------DirectoryName: " + path.getName());
					System.out.println("------------DirectoryName: " + path.getName());
				}
				else if (fs.isFile(path)){
					LOG.info("-----------FileName: " + path.getName());
					System.out.println("-----------FileName: " + path.getName());
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.info("list fileSystem object stream.: ", e);
			e.printStackTrace();
		}
		
	}
	
	public void mkdir(String dir){
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name","hdfs://192.168.163.131:9000");
		FileSystem fs = null;
		try {
			fs = FileSystem.get(configuration);
			Path path = new Path(dir);
			if(!fs.exists(path)){
				fs.mkdirs(path);
				System.out.println("create directory " + dir + " successfully!");
			}else{
				System.out.println("directory " + dir + " exists!");
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(fs !=null ){
				try {
					fs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}    
	}
	
	public void readFile(String file){
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name","hdfs://192.168.163.131:9000");
		FileSystem fs;
		try {
			fs = FileSystem.get(configuration);
			Path path = new Path(file);
			if(!fs.exists(path)){
				System.out.println("file " + " doesn't exist!");
				return;
			}
			FSDataInputStream  in= fs.open(path);
			String filename = file.substring( file.lastIndexOf('/') + 1, file.length());
			OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(filename)));
			byte[] b = new byte[1024];
			int numBytes = 0;
			while((numBytes = in.read(b)) > 0){
				out.write(b, 0, numBytes);
			}
			in.close();
			out.close();
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean ifExists(String source){
		if(source == null || source.length() == 0){
			return false;
		}
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name","hdfs://192.168.163.131:9000");
		FileSystem fs = null;
		try {
			fs = FileSystem.get(configuration);
			return fs.exists(new Path(source));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}finally {
			if( fs != null){
				try {
					fs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	/*
	 * Recursively copies the source path directories or files to the destination file of HDFS,
	 * that is merging multiple files of local path into a file of HDFS.
	 * It is the same functionality as the following command:
	 *  hadoop fs -putMerge <local fs><hadoop fs>
	 */
	public void putMerge(String source, String dest){
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name","hdfs://192.168.163.131:9000");
		try {
			FileSystem localFS = FileSystem.getLocal(configuration);
			FileSystem hdfsFS = FileSystem.get(configuration);
			Path localPath = new Path(source);
			Path hdfsPath = new Path(dest);
			FSDataOutputStream out = hdfsFS.create(hdfsPath);
			if(!localFS.exists(localPath)){
				System.out.println("file " + " doesn't exist!");
				return;
			}
			FileStatus[] status = localFS.listStatus(localPath);
			for (FileStatus fileStatus: status){
				Path local = fileStatus.getPath();
				FSDataInputStream  in = localFS.open(local);
				byte[] buffer = new byte[1024];
				int numBytes = 0;
				while((numBytes = in.read(buffer)) > 0){
					out.write(buffer, 0, numBytes);
				}
				System.out.println(local + " has been copoed!");
				in.close();
			}
			out.close();
			localFS.close();	
			hdfsFS.close();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	 * Recursively copies the source path directories or files to the destination path of HDFS
	 * It is the same functionality as the following command:
	 *      hadoop fs -copyFromLocal <local fs><hadoop fs>
	 * Briefly, copy folder into folder
	 * @param source
	 * @param dest
	 */
	public void copyFromLocal (String source, String dest){
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		FileSystem fs;
		try {
			fs = FileSystem.get(configuration);
			Path srcPath = new Path(source);
			Path dstPath = new Path(dest);
			
			//check if the file already exists
			if(!(fs.exists(dstPath))){
				System.out.println("dstPath doesn't exist");
				System.err.println("No such destination " + dstPath);
				return;
			}
			
			//Get the filename out of the file path
			String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
			
			try {
				//if the file exists in the destination path, it will throw exception.fs.copyFromLocalFile(srcPath, dstPath)
				//remove and overwrite files with the method
				//copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
				fs.copyFromLocalFile(false, true, srcPath, dstPath);
				System.out.println("File " + filename + " copies to " + dest);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally{
				fs.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	
	public void renameFile(String fromthis, String tothis){
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		FileSystem fs;
		try {
			fs = FileSystem.get(configuration);
			Path fromPath = new Path(fromthis);
			Path toPath = new Path(tothis);
			
			if(!(fs.exists(fromPath))){
				System.out.println("No such destination " + fromPath);
				return;
			}
			
			if(fs.exists(toPath)){
				System.out.println("Already exists: " + toPath);
				return;
			}
			
			try {
				boolean isRenamed = fs.rename(fromPath, toPath); //rename file name indeed.
				if(isRenamed){
					System.out.println("Renamed from " + fromthis + " to " + tothis);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	 * Uploads or adds a file to HDFS
	 */
	public void addFile(String source, String dest){
		//Conf object will read the HDFS configuration parameters
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		FileSystem fs;
		try {
			fs = FileSystem.get(configuration);
			
			//Get the filename out of the file path
			String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
			
			//Create the destination path including the filename.
			if (dest.charAt(dest.length()-1) != '/'){
				dest = dest + "/" + filename;
			}else{
				dest = dest + filename;
			}
			
			//Check if the file already exists
			Path path = new Path(dest);
			if(fs.exists(path)){
				System.out.println("file " + dest + " already exists");
				return ;
			}
			
			//Create a new file and write data to it
			FSDataOutputStream out = fs.create(path);
			InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));
			byte[] buffer = new byte[1024];
			int numBytes = 0;
			//In this way, read and write data to destination file.
			while((numBytes = in.read(buffer)) > 0){
				out.write(buffer, 0, numBytes);
			}
			in.close();
			out.close();
			fs.close();
 		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	 * Deletes the files if it is a directory
	 */
	public void deleteFile(String file){
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		try {
			FileSystem fs = FileSystem.get(configuration);
			Path filePath = new Path(file);
			if(!(fs.exists(filePath))){
				System.out.println("file " + file + "does not exist");
				return;
			}
			//delete(Path f, boolean recursive), the second parameter is to set whether we delete files recursively.
			fs.delete(filePath, true);
			System.out.println("files have been deleted");
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	
	/*
	 * Gets the information about the file modified time.
	 */
	public void getModificationTime(String source) throws IOException{
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		FileSystem fs = FileSystem.get(configuration);
		Path srcPath = new Path(source);
		
		//Check if the file already exists
		if(!(fs.exists(srcPath))){
			System.out.println("file " + srcPath + " does not exist");
		}
		
		//Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
		FileStatus status = fs.getFileStatus(srcPath);
		long modificationTime = status.getModificationTime();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date(modificationTime);		
		System.out.println( filename + "is modified in : " + simpleDateFormat.format(date));
		
	}
	
	/*
	 * Gets the file block locations info
	 */
	public void getBlockLocations(String source) {
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		try {
			FileSystem fs = FileSystem.get(configuration);
			Path srcPath = new Path(source);
			
			//Check if the file already exists
			if(!(fs.exists(srcPath))){
				System.out.println( srcPath + " does not exist");
			}
			
			//Get the filename out of the file path
			String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
			
			FileStatus status = fs.getFileStatus(srcPath);

			BlockLocation[]  blocks = fs.getFileBlockLocations(srcPath, 0, status.getLen());
			
			for (BlockLocation block: blocks){
				String[] hosts = block.getHosts();
				for (int i = 0; i < hosts.length; i++){
					System.out.println("filename is: " + filename + ", block is: " + block + ", host ip is: " + hosts);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
 	}
	
	
	public void getHostnames() throws IOException{
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		FileSystem fs = FileSystem.get(configuration);
		/*
		 * Notice: the result of FileSystem.get(conf) is a instance of LocalFileSystem.
		 */
		
		DistributedFileSystem hdfs = (DistributedFileSystem)fs;
		DatanodeInfo[] datanodeInfos = hdfs.getDataNodeStats();
		
		String[] names = new String[datanodeInfos.length];
		for (int i=0; i < datanodeInfos.length; i++){
			names[i] = datanodeInfos[i].getHostName();
			System.out.println(names[i]);
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		Basic basicOperation = new Basic();
		System.out.println("test1");
		//basicOperation.list("/input/");
		//basicOperation.mkdir("/mkTest/erji");
		//basicOperation.readFile("/input/action.txt");
		//System.out.println(basicOperation.ifExists("/input/action"));
		//basicOperation.putMerge("F://code/eclipse/input", "/input/copyFiles");
		//basicOperation.renameFile("/input/action.txt", "/input/actionrename.txt");
		//basicOperation.addFile("F://code/eclipse/input/longText.txt", "/input/");
		//basicOperation.deleteFile("/input/longText.txt");  //delete a file
		//basicOperation.deleteFile("/input/test");    //delete a directory and files of this directory.
		/*try {
			basicOperation.getModificationTime("/input/longText.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		//basicOperation.getBlockLocations("/input/partition.txt");
		basicOperation.getHostnames();
		System.out.println("test2");
	}

}
