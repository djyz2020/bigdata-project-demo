package com.practice.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDemo {
	
	//获取table
	public static HTable getTable(String tableName) throws IOException{
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "stu_info");
		return table;
	}
	
	//查询数据
	public static void getData(HTable table) throws IOException{
		//第一种获取数据的方法: 根据列簇和列分隔符获取
		/*String family = "info";
		String qualifier = "age";
		ResultScanner sc = table.getScanner(family.getBytes(), qualifier.getBytes());
		Iterator<Result> iter = sc.iterator();
		while(iter.hasNext()){
			Result result = iter.next();
			System.out.println("--------------------------------------------------");
			System.out.println(Bytes.toString(result.getRow()));
			List<Cell> list = result.listCells();
			for(Cell cell : list){
				System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + ":" 
						+ Bytes.toString(CellUtil.cloneQualifier(cell)) + " =>" 
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
			System.out.println("--------------------------------------------------");
		}*/
		//第二种获取属性方法： 根据行键取
		Result rs = table.get(new Get("20180401_10001".getBytes()));
		List<Cell> list = rs.listCells();
		for(Cell cell : list){
			System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + ":" 
					+ Bytes.toString(CellUtil.cloneQualifier(cell)) + " =>" 
					+ Bytes.toString(CellUtil.cloneValue(cell)));
		}
	}
	
	//扫描表
	public static void scanTable(HTable table) throws IOException{
		Scan scan = new Scan();
		ResultScanner rss = table.getScanner(scan);
		Iterator<Result> iter = rss.iterator();
		while(iter.hasNext()){
			Result result = iter.next();
			System.out.println("--------------------------------------------------");
			System.out.println(Bytes.toString(result.getRow()));
			List<Cell> list = result.listCells();
			for(Cell cell : list){
				System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + ":" 
						+ Bytes.toString(CellUtil.cloneQualifier(cell)) + " =>" 
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
			System.out.println("--------------------------------------------------");
		}
	}
	
	public static void putData(HTable table) throws IOException{
		Put put = new Put("20180401_10001".getBytes());
		put.add("info".getBytes(), "sex1".getBytes(), "UU".getBytes());
		table.put(put);
		getData(table);
	}
	
	public static void deleteData(HTable table) throws IOException {
		Delete delete = new Delete("20170222_10001".getBytes());
		delete.deleteColumn("info".getBytes(), "name".getBytes());
		table.delete(delete);
	}
	
	public static void main(String[] args) throws IOException {
		//1. hbase查询数据
		getData(getTable("stu_info")); 
		//2. hbase增加数据
		//putData(getTable("stu_info"));
		//3. hbase删除数据
		//deleteData(getTable("stu_info"));
		//4. hbase扫描全表
		//scanTable(getTable("stu_info"));
		
	}

}
