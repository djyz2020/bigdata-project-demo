package com.bigdata.hive01;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class TestDateUDF extends UDF{
	
	/**
	 * 日期格式转换 当前日期格式：31/Aug/2015:00:04:37 +0800 目标日期格式：2015-08-31 00:04:37
	 * 
	 * @param time
	 * @return
	 * @throws ParseException 
	 */
	// 注意建议使用public，因为要将程序打jar包进行调用的
	public Text evaluate(Text time) throws ParseException{
		if(time == null){
			return null;
		}
		if(StringUtils.isBlank(time.toString())){
			return null;
		}
		SimpleDateFormat inputDate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		SimpleDateFormat outputDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String cDate = time.toString().replace("\"", "");
		Date iDate = inputDate.parse(cDate);
		return new Text(outputDate.format(iDate));
	}
	
	
	public static void main(String[] args) {
		try {
			System.out.println(new TestDateUDF().evaluate(new Text("31/Aug/2015:00:04:37 +0800")));
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	

}
