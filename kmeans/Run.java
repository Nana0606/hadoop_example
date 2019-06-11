
/*
 * 代码来源：http://blog.csdn.net/garfielder007/article/details/51612730
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Run {
	
	private static String FLAG = "KCLUSTER";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
		
		Path kMeansPath = new Path("/input/center.txt");   //初始的质心文件
		Path samplePath = new Path("/input/sampleCenters.txt");   //样本文件
		//加载聚类中心文件
		Center center = new Center();
		String centerString = center.loadInitCenter(kMeansPath);
		System.out.println("test1");
		
		int index = 0;  //迭代次数
		while( index < 5){
			
			configuration = new Configuration();
			configuration.set("fs.default.name", "hdfs://192.168.163.131:9000");
			configuration.set(FLAG, centerString);  //将聚类中心的字符串放到configuration中
			
			kMeansPath = new Path("/output/kmeansOut/kmeans" + index); //本次迭代的输出路口，也是下一次质心的读取路径
			System.out.println("test2");
			
			//判断输出路径是否存在，如果存在则删除
			FileSystem hdfs = FileSystem.get(configuration);
			if(hdfs.exists(kMeansPath))  hdfs.delete(kMeansPath, true);
			System.out.println("test3");
			
			Job job = Job.getInstance(configuration, "kmeans" + index);
			job.setJarByClass(Run.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, samplePath);  
	        FileOutputFormat.setOutputPath(job, kMeansPath);  
	        job.waitForCompletion(true);  
	        System.out.println("test4");
	        
	        /*
	         * 获取自定义center大小，如果等于质心的大小，说明质心已经不会发生变化了，则程序停止迭代
	         */
	        long counter = job.getCounters().getGroup("myCounter").findCounter("kmeansCounter").getValue();  
            if(counter == Center.k) System.exit(0);  
            /**重新加载质心*/  
            center = new Center();  
            System.out.println("test5");
            centerString = center.loadCenter(kMeansPath);  
            index ++;  
		}
		System.exit(0);  
	}

}
