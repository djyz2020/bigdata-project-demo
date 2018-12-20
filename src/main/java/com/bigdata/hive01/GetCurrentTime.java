package com.bigdata.hive01;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class GetCurrentTime extends UDF{
	
	// 注意建议使用public，因为要将程序打jar包进行调用的
	public Text evaluate() throws ParseException{
		Date iDate = new Date();
		SimpleDateFormat outputDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return new Text(outputDate.format(iDate));
	}
	
	
	public static void main(String[] args) {
		try {
			System.out.println(new GetCurrentTime().evaluate());
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
