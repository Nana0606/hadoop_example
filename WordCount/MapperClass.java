import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper<Object, Text, Text, IntWritable>的4个参数，前2个是输入的类型，后2个是输出的类型
//java中的String类型在Hadoop中使用Text代替，java中的int类型在Hadoop中使用IntWritable代替
public class MapperClass extends Mapper<Object, Text, Text, IntWritable>{

	public Text keyText = new Text("key"); //String keyText = "key"，用于存储key值
	public IntWritable intValue = new IntWritable(1);  //用于存储value值
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//step1: 获取值
		String str = value.toString();
		//step2: 分割，StringTokenizer()默认使用空格拆分
		StringTokenizer stringTokenizer = new StringTokenizer(str);
		//step3: 
		while(stringTokenizer.hasMoreTokens()){
			keyText.set(stringTokenizer.nextToken());
			context.write(keyText, intValue);  //context.write("My",1);
		}

	}	
	
}
