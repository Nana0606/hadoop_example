import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommonReduce extends Reducer<TextPair, Text, Text, Text> {
	
	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("进入reduce");
		String tradeId = values.iterator().next().toString(); //first value is tradeID
		System.out.println(tradeId);
		while(values.iterator().hasNext()){
			//next is payID
			String payID = values.iterator().next().toString();
			context.write(new Text(tradeId), new Text(payID));
		}
	}

}
