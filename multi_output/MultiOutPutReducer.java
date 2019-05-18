package com.itcast.hadoop.muliti;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


//输出到多个文件或多个文件夹，驱动中不需要任何改变，只需要加个multipleOutputs的变量和setup()、cleanup()函数
//然后就可以用mos.write(Key key,Value value,String baseOutputPath)代替context.write(key, value); 
public class MultiOutPutReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> multipleOutputs = null;

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for (Text text : values) {
			//存到一个文件中，文件名为：KeySplit-r-00000，内容为：key置空，value是key的值
			//即，正常输出key-value是“1101-1101,thinkpad yoga,联想,windows 8,超级本”，这里只显示“1101/”。
			multipleOutputs.write("KeySplit", NullWritable.get(), key.toString() + "/"); 
			//存到另一个文件中，文件名为：KeySplit-r-00000，内容为：key置空，value是value的值
			//即，正常输出key-value是“1101-1101,thinkpad yoga,联想,windows 8,超级本”，这里只显示“1101,thinkpad yoga,联想,windows 8,超级本”。
			multipleOutputs.write("AllPart", NullWritable.get(), text); 
		}
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(null != multipleOutputs){
			multipleOutputs.close();
			multipleOutputs = null;
		}
	}
	
}
