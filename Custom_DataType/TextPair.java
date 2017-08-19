import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

//自定义的IO类型
public class TextPair implements Writable{

	//想要将2个类型的key分开
	public int id;
	public String name;
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	//自定义数据类型主要是重写write()和readFields()函数
	@Override
	public void write(DataOutput in) throws IOException {
		// TODO Auto-generated method stub
		in.writeInt(id);
		in.writeUTF(name);   //需要写入string类型的函数，这里使用writeUTF()
	}
	
	@Override
	public void readFields(DataInput out) throws IOException {
		// TODO Auto-generated method stub
		id = out.readInt();
		name = out.readUTF();   //同样读取string类型的函数使用readUTF()
	
	}


}
