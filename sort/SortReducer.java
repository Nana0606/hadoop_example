import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
*主要功能是将同一个key对应的value值整合，整合成value1,value2,value3的形式
*/
public class SortReducer extends Reducer<IntPaire, IntWritable, Text, Text>{

	
	protected void reduce(IntPaire key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		StringBuffer resultValue = new StringBuffer();
		//String newString = "";
		Iterator<IntWritable> iterator = values.iterator();
		while(iterator.hasNext()){
			int value = iterator.next().get();
			resultValue.append(value + ",");
		}
		int length = resultValue.length();
    //这个代码主要是处理多个value值中最后一个value值后的逗号问题
		if(resultValue.length() > 0){
			resultValue = resultValue.delete(length-1, length);
			//newString = combineValue.substring(0, length-1);
		}
		context.write(new Text(key.getFirstKey()), new Text(resultValue.toString()));
		//context.write(new Text(key.getFirstKey()), new Text(newString));
	}
	
	

}
