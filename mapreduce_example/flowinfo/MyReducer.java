package org.apache.hadoop.mapreduce.app;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, DataWritable, Text, DataWritable>{

	DataWritable data = new DataWritable();
	
	@Override
	protected void reduce(Text key, Iterable<DataWritable> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//System.out.println("调用了reduce函数");
		 int sumUpPackNum = 0;
		 int sumUpPayLoad = 0;
		 int sumDownPackNum = 0;
		 int sumDownPayLoad = 0;
		 for(DataWritable value: values){
			 sumUpPackNum += value.getUpPackNum();
			 sumUpPayLoad += value.getUpPayLoad();
			 sumDownPackNum += value.getDownPackNum();
			 sumDownPayLoad += value.getDownPayLoad();
          }         
		 data.set(sumUpPackNum, sumUpPayLoad, sumDownPackNum, sumDownPayLoad);
         context.write(key,data);   
        // System.out.println("reduce函数中写入成功");
	}
	
	

}
