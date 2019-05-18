import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, Text, Text>{

	public Text keyText = new Text("key"); //String keyText = "key"
	public Text value = new Text("value");
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//step1: 获取值
		String str = value.toString();
		System.out.println(str);
		//step2: 分割，将字符串使用split分割
		String[] strs = str.split(" ");
		//step3:根据提供的源文件的长度，即若是com 1 2即由3个字符串组成，我们认为是right，2个是short，4个是long
		if(strs.length == 2){
			keyText.set("short");
			value.set(str);
			//System.out.println(keyText + ":" + value);
			context.write(keyText, value);
		}else if (strs.length == 3){
			keyText.set("right");
			value.set(str);
			//System.out.println(keyText + ":" + value);
			context.write(keyText, value);
		}else {
			keyText.set("long");
			value.set(str);
			//System.out.println(keyText + ":" + value);
			context.write(keyText, value);
		}

	}	
	
}
//最后都写成了short 1 1,这种相当于在前面加上了标签

