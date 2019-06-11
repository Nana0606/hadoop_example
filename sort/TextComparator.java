import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextComparator extends WritableComparator{
	
	public TextComparator(){
		super(IntPaire.class, true);
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		IntPaire o1 = (IntPaire) a;
		IntPaire o2 = (IntPaire) b;
		return o1.getFirstKey().compareTo(o2.getFirstKey());
	}

}
