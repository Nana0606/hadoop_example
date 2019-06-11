import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class IntPaire implements WritableComparable<IntPaire>{
	
	private String firstKey;  //Str1
	private int SecondKey;  //5

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		firstKey = in.readUTF();    //String类型使用readUTF()读取
		SecondKey = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub	
		out.writeUTF(firstKey);  //String类型使用writeUTF()读取
		out.writeInt(SecondKey);	
	}

	@Override
	public int compareTo(IntPaire o) {
		// TODO Auto-generated method stub
		return o.getFirstKey().compareTo(this.firstKey);
	}

	public String getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(String firstKey) {
		this.firstKey = firstKey;
	}

	public int getSecondKey() {
		return SecondKey;
	}

	public void setSecondKey(int secondKey) {
		SecondKey = secondKey;
	}
	
}
