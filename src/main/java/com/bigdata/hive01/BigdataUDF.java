package com.bigdata.hive01;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * A User-defined function (UDF) for the use with Hive.
 *
 * New UDF classes need to inherit from this UDF class.
 *
 * Required for all UDF classes: 1. Implement one or more methods named
 * "evaluate" which will be called by Hive. The following are some examples:
 * public int evaluate(); public int evaluate(int a); public double evaluate(int
 * a, double b); public String evaluate(String a, int b, String c);
 *
 * "evaluate" should never be a void method. However it can return "null" if
 * needed.
 */

public class BigdataUDF extends UDF{
	
	public Text evaluate(Text str){
		return evaluate(str, new IntWritable(0));
	}
	
	public Text evaluate(Text str, IntWritable flag){
		if(str != null){
			if(flag.get() == 0){
				return new Text(str.toString().toLowerCase());
			}else if(flag.get() == 1){
				return new Text(str.toString().toUpperCase());
			}else{
				return null;
			}
		}
		return null;
	}
	
//	public static void main(String[] args) {
//		System.out.println(new BigdataUDF().evaluate(new Text("HADOOP")));
//	}

}
