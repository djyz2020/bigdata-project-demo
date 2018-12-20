package com.bigdata.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;

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
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest01 {
	
	/**
	 * 获取hbase数据表连接
	 * @param name 数据表名称
	 * @return HTable
	 * @throws IOException
	 */
	public static HTable getTable(String name) throws IOException{
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, name);
		return table;
	}
	
	/**
	 * get 查询数据
	 * @param table 数据表
	 * @throws IOException
	 */
	public static void getData(HTable table) throws IOException{
		Get get = new Get(Bytes.toBytes("20180221"));
		//get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"));
		get.addFamily(Bytes.toBytes("f1"));
		Result rs = table.get(get);
		//print data
		for(Cell cell : rs.rawCells()){
			System.out.println(
					Bytes.toString(CellUtil.cloneFamily(cell)) 
							+ "->" + Bytes.toString(CellUtil.cloneQualifier(cell))
							+ "->" + Bytes.toString(CellUtil.cloneValue(cell)) 
							+ "->" + cell.getTimestamp());
			System.out.println("---------------------------------------------");
		}
	}
	
	/**
	 * put 添加数据
	 * @param table
	 * @throws InterruptedIOException 
	 * @throws RetriesExhaustedWithDetailsException 
	 */
	public static void putData(HTable table) throws RetriesExhaustedWithDetailsException, InterruptedIOException{
		Put put = new Put(Bytes.toBytes("20180221"));
		put.add(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("haibozhang"));
		table.put(put);
		//get the data
		try {
			getData(table);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * delete 删除数据
	 * @param table 数据表
	 * @throws Exception
	 */
	public static void deleteData(HTable table) throws Exception {
		Delete del = new Delete(Bytes.toBytes("20180221"));
		del.deleteColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));
		table.delete(del);
		getData(table);
	}
	
	/**
	 * scan 扫描表
	 * @param table
	 * @throws IOException
	 */
	public static void scanData(HTable table) throws IOException{
		long startTime = System.currentTimeMillis();
		Scan scan = new Scan();
		//conf the scan
		scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));
		scan.setStartRow(Bytes.toBytes("20170521_10001"));
		//scan.setStopRow(Bytes.toBytes("20170521_10003"));
		
		ResultScanner rsscan = table.getScanner(scan);
		for(Result rs : rsscan){
			System.out.println(Bytes.toString(rs.getRow()));
			for(Cell cell : rs.rawCells()){
				System.out.println(Bytes.toString(CellUtil.cloneFamily(cell))
						+ " -> " + Bytes.toString(CellUtil.cloneQualifier(cell))
						+ " -> " + Bytes.toString(CellUtil.cloneValue(cell))
						+ "->" + cell.getTimestamp());
			}
			System.out.println("---------------------------------------------");
		}
		System.out.println("耗时：" + (System.currentTimeMillis() - startTime) + "ms");
	}
	
	public static void main(String[] args) throws Exception {
		HTable table = getTable("ns1:tb1");
		//getData(table);
		//putData(table);
		//deleteData(table);
		scanData(table);
	}

}
