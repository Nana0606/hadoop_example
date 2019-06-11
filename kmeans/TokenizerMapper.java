import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
	
	double[][] centers = new double[Center.k][];
	String[] centerstrArray = null;
	private static String FLAG = "KCLUSTER";
	
	@Override
	protected void setup(Context context) {
		// TODO Auto-generated method stub
		//将放在context中的聚类中心转换为数组的形式，方便使用（质心）
		String kmeansS = context.getConfiguration().get(FLAG);
		centerstrArray = kmeansS.split("\t");
		for ( int i = 0; i < centerstrArray.length; i++){
			String[] segs = centerstrArray[i].split(",");
			centers[i] = new double[segs.length];
			for (int j = 0; j < segs.length; j++){
				centers[i][j] = Double.parseDouble(segs[j]);
			}
		}
				
	}
	
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line = value.toString();
		String[] segs = line.split(",");
		double[] sample = new double[segs.length];
		for ( int i = 0; i < segs.length; i++){
			sample[i] = Float.parseFloat(segs[i]);
		}
		//求得距离最近的质心
		double min = Double.MAX_VALUE;
		int index = 0;
		for ( int i = 0; i < centers.length; i++){
			double dis = distance(centers[i], sample);
			if( dis < min){
				min = dis;
				index = i;
			}
		}
		context.write(new Text(centerstrArray[index]), new Text(line));
	}
			
  public static double distance(double[] a, double[] b){
		if( a == null || b == null || a.length != b.length)  return Double.MAX_VALUE;
		double dis = 0;
		for ( int i = 0; i < a.length; i++){
			dis += Math.pow(a[i] - b[i], 2);
		}
		return Math.sqrt(dis);
	}

}
