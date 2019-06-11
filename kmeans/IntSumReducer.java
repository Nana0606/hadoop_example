import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;


public class IntSumReducer extends Reducer<Text, Text, NullWritable, Text>{
	
	Counter counter = null;

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		double[] sum = new double[Center.k];
		int size = 0;
		
		//计算对应维度上值的加和，存放在sum数组中
		for (Text text: values){
			String[] segs = text.toString().split(",");
			for ( int i = 0; i < segs.length; i++){
				sum[i] += Double.parseDouble(segs[i]);
			}
			size ++;
		}
		
		//求sum数组中每个维度的平均值，也就是新的质心
		StringBuffer sBuffer = new StringBuffer();
		for (int i = 0; i < sum.length; i++){
			sum[i] /= size;
			sBuffer.append(sum[i]);
			sBuffer.append(",");
		}
		
		
		//判断新的质心跟老的质心是否是一样的
		boolean flag = true;
		//centerstrArray是老的质心
		String[] centerstrArray = key.toString().split(",");
		for ( int i =0; i < centerstrArray.length; i++){
			if(Math.abs(Double.parseDouble(centerstrArray[i]) - sum[i]) > 0.00000000001){
				flag = false;
				break;
			}
		}
		
		//如果新的质心跟老的质心是一样的，那么相应的计算器加1
		if(flag){
			counter = context.getCounter("myCounter", "kmeansCounter");
			counter.increment(1l);
		}
		//输出的新的质心
		context.write(null,new Text(sBuffer.toString()));
	}
	
}
