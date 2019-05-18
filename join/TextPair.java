import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair>{
	
	private String text;  //ID
	private int id;  //标识商品表和支付表，商品表用0标识，支付表用1标识

	public TextPair(String text, int id) {
		// TODO Auto-generated constructor stub
		this.id = id;
	}
	
	public TextPair() {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		text = in.readUTF();
		id = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(text);
		out.writeInt(id);
	}

	@Override
	public int compareTo(TextPair o) {
		// TODO Auto-generated method stub
		return o.getText().compareTo(this.getText());
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public int getID() {
		return id;
	}

	public void setID(int id) {
		this.id = id;
	}

}
