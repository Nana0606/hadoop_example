package org.apache.hadoop.mapreduce.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class DataWritable implements Writable {
	
	// 手机号码作为key输出，不需要在此定义
	// upload
	private int upPackNum;
	private int upPayLoad;
	
	//download
	private int downPackNum;
	private int downPayLoad;
	
	public DataWritable() {
		// TODO Auto-generated constructor stub
	}
	
	public void set(int upPackNum, int upPayLoad, int downPackNum, int downPayLoad){
		this.upPackNum = upPackNum;
		this.upPayLoad = upPayLoad;
		this.downPackNum = downPackNum;
		this.downPayLoad = downPayLoad;
	}

	
	
	public int getUpPackNum() {
		return upPackNum;
	}

	public int getUpPayLoad() {
		return upPayLoad;
	}

	public int getDownPackNum() {
		return downPackNum;
	}

	public int getDownPayLoad() {
		return downPayLoad;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(upPackNum);
		out.writeInt(upPayLoad);
		out.writeInt(downPackNum);
		out.writeInt(downPayLoad);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.upPackNum = in.readInt();
		this.upPayLoad = in.readInt();
		this.downPackNum = in.readInt();
		this.downPayLoad = in.readInt();

	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return upPackNum + "\t" + upPayLoad + "\t" + downPackNum + "\t" + downPayLoad;
	}
	
}
