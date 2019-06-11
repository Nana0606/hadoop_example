import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
	
	LogParser logParser = new LogParser();
	Text outputValue = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		final String[] parsed = logParser.parse(value.toString());
		
		//step1: 过滤掉静态资源访问请求
		if((parsed[2].startsWith("GET /static/"))||(parsed[2].startsWith("GET /uc_server"))){
			return;
		}
		
		//step2：过滤掉开头的指定字符串
		if(parsed[2].startsWith("GET /")){
			parsed[2] = parsed[2].substring("GET /".length());
		} else if(parsed[2].startsWith("POST /")){
			parsed[2] = parsed[2].substring("POST /".length());
		}
		
		//step3:过滤掉结尾的特定字符串
		if(parsed[2].endsWith(" HTTP/1.1")){
			parsed[2] = parsed[2].substring(0, parsed[2].length() - " HTTP/1.1".length());
		}
		
		//step4： 只写入前3个记录类型项
		outputValue.set(parsed[0] + "\t" + parsed[1] + "\t" + parsed[2]);
	    context.write(key, outputValue);
	}
	
	

}
