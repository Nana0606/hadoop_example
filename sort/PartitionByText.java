import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionByText extends Partitioner<IntPaire, IntWritable>{

	@Override
	public int getPartition(IntPaire key, IntWritable value, int numPartition) { //numPartition为reducer的个数
		// TODO Auto-generated method stub
		return (key.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartition;
	}
	

}
