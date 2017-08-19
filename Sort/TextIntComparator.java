package com.itcast.hadoop.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextIntComparator extends WritableComparator{
	
	public TextIntComparator(){
		super(IntPaire.class, true);    //注册comparator
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		IntPaire o1 = (IntPaire) a;
		IntPaire o2 = (IntPaire) b;
		if (!o1.getFirstKey().equals(o2.getFirstKey())) {
			return o1.getFirstKey().compareTo(o2.getFirstKey());
		} else {
			return o1.getSecondKey() - o2.getSecondKey();
		}
	}

	
	
}
