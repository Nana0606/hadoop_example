import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


//考虑的时候只需要考虑一个特殊的key值即可，mapreduce思想是循环操作的
//对于mapper输出为i 1, am 2, i 2的输出，reducer的输入是i (1,2), am 2;
public class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable>{

	public IntWritable intValue = new IntWritable(0);
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, // name [1,1,1]
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		//step1，对于某一key，将所有value的值想加，即得到这个单词出现的所有次数
		int sum = 0;
		while(values.iterator().hasNext()){
			sum += values.iterator().next().get();
		}
		intValue.set(sum);
		context.write(key, intValue);  //切记别忘记写入
	}
	
}
