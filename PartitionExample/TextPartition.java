import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextPartition extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int numPartition) {
		// TODO Auto-generated method stub
		
		int result = 1;
		System.out.println(key + " " + key.toString());
		System.out.println(key.equals("short") + " " + key.toString().equals("short"));
		System.out.println(key.equals(new Text("short")) + " " + key.toString().equals("short"));
    //key.toString().equals("short")这个是调试好久才改正过来的，之前的bug是直接写成key.equals("short")
		if(key.toString().equals("short")){
			result = 0 % numPartition;   //输出文件名：part-r-00000
		}else if(key.toString().equals("right")){
			result = 1 % numPartition;   //输出文件名：part-r-00001
		}else if(key.toString().equals("long")){
			result = 2 % numPartition;   //输出文件名：part-r-00002
		}
		
		return result;
		//return key.toString().length() % numPartition;
	}
	
}
