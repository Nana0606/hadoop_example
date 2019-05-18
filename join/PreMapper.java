import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.itcast.hadoop.join.TextPair;



public class PreMapper extends Mapper<Object, Text, TextPair, Text>{

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//  path = /input/action.txt
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String path = fileSplit.getPath().toString();
		String[] line = value.toString().split("\"");  //action.txt alipay.txt
		if(line.length < 2){
			// skip bad value return;
			return;
		}
		TextPair kr = new TextPair();
		String productID = line[0];
		kr.setText(productID);
		
		//set product ID;
		Text kv = new Text();
    //若是action文件
		if(path.indexOf("action") >= 0){
			//trade table
			kr.setID(0);
			//用于TextPair的排序
			String tradeID = line[1];
			kv.set(tradeID);
			//value is tradeID
		} 
    else if(path.indexOf("alipay") >= 0){   //若是alipay文件
			//pay table
			kr.setID(1);
			//用于TextPair排序
			String payID = line[1];
			kv.set(payID);
			//value is payID
		}
		//System.out.println("mapper写入开始");
		context.write(kr, kv);
		//System.out.println("mapper写入完成");
	}

}
