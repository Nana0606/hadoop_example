package com.itcast.hadoop.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FirstComparator extends WritableComparator{

	public FirstComparator(){
		super(TextPair.class, true);
		System.out.println("进入firstcomparator-初始化函数");
	}
	
	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		System.out.println("进入firstcomparator-compare函数");
		TextPair o1 = (TextPair) a;
		TextPair o2 = (TextPair) b;
		return o1.getText().compareTo(o2.getText());
	}
}
