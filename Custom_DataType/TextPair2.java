import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/*
*需要完善函数：write(), readFields(), compareTo(), hashCode(), equals(), toString()以及Comparator
*/
public class TextPair2 implements WritableComparable<TextPair2> {
	
	private Text first;
	private Text second;
	
	public TextPair2() {
		// TODO Auto-generated constructor stub
	}
	
	public TextPair2(Text first, Text second){
		this.set(first, second);
	}
	
	public void set(Text first, Text second){
		this.first = first;
		this.second = second;
	}

	 
	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
        second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
        first.readFields(in);
        second.readFields(in);
	}

	@Override
	public int compareTo(TextPair2 o) {
		// TODO Auto-generated method stub
		int cmp = this.first.compareTo(o.getFirst());
		if(0 != cmp){
			return cmp;
		}else{
			return this.second.compareTo(o.second);
		}
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if(obj instanceof TextPair2){
			TextPair2 tp2 = (TextPair2)obj;
			return first.equals(tp2.first) && second.equals(tp2.second);
		}
		return false;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return first + "\t" + second;
	}
	
	public static class Comparator extends WritableComparator{

		public Comparator() {
			// TODO Auto-generated constructor stub
			super(TextPair2.class);
		}
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// TODO Auto-generated method stub
			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
		    int n2 = WritableUtils.decodeVIntSize(b2[s2]);
		    int cmp = WritableComparator.compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
		    
		    if(0 != cmp){
		    	return cmp;
		    }
			
			int thisValue = readInt(b1, s1);
		    int thatValue = readInt(b2, s2);
		    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
		}
		
		static{
			WritableComparator.define(TextPair2.class, new Comparator());
		}
		
	}
	
	
	

}
