package com.bigdata.hive;

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

/**
 * 需求：转换大小写，0代表小写，1代表大写，默认转换小写
 * 
 * @author ibf
 *
 */
public class BigDataUdf extends UDF {

	public Text evaluate(Text str) {
		return this.evaluate(str, new IntWritable(0));
	}

	public Text evaluate(Text str, IntWritable flag) {

		// 如果不等于null，就进行转换大小写的操作，说明数据合法
		if (str != null) {
			// 如果等于0，表示转换小写
			if (flag.get() == 0) {
				return new Text(str.toString().toLowerCase());
				// 如果等于1，表示转换大写
			} else if (flag.get() == 1) {
				return new Text(str.toString().toUpperCase());
			} else
				return null;
		} else
			return null;
	}

	public static void main(String[] args) {

		System.out.println(new BigDataUdf().evaluate(new Text("hadoop"), new IntWritable(1)));
	}
}
