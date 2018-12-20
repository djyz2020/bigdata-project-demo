package com.bigdata.hbase;

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

public class HbaseDemo {
	
	public static HTable getTable(String name) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, name);

		return table;
	}

	/**
	 * get 'tb_name' 'rowkey' 'cf:col'
	 * 
	 * @param table
	 * @throws Exception
	 */
	public static void getData(HTable table) throws Exception {

		Get get = new Get(Bytes.toBytes("20170521_10003"));
		// get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
		get.addFamily(Bytes.toBytes("info"));

		Result rs = table.get(get);
		// print the data
		for (Cell cell : rs.rawCells()) {
			System.out.println(
					Bytes.toString(CellUtil.cloneFamily(cell)) + "->" + Bytes.toString(CellUtil.cloneQualifier(cell))
							+ "->" + Bytes.toString(CellUtil.cloneValue(cell)) + "->" + cell.getTimestamp());
			System.out.println("---------------------------------------------");
		}
		// load the get
		table.get(get);
	}

	/**
	 * put 'tb_name' 'rowkey' 'cf:col' 'value'
	 * 
	 * @param table
	 * @throws Exception
	 * @throws RetriesExhaustedWithDetailsException
	 */
	public static void putData(HTable table) throws Exception {

		Put put = new Put(Bytes.toBytes("20170521_10003"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"));

		table.put(put);
		//get the data
		getData(table);
	}

	public static void deleteData(HTable table) throws Exception {
		Delete del = new Delete(Bytes.toBytes("20170521_10003"));
		del.deleteColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
		table.delete(del);
		getData(table);
	}

	public static void scanData(HTable table) throws Exception {

		Scan scan = new Scan();

		// load the scan
		ResultScanner rsscan = table.getScanner(scan);

		for (Result rs : rsscan) {
			System.out.println(Bytes.toString(rs.getRow()));
			for (Cell cell : rs.rawCells()) {
				System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + "->"
						+ Bytes.toString(CellUtil.cloneQualifier(cell)) + "->"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "->" + cell.getTimestamp());
			}
			System.out.println("---------------------------------------------");
		}
	}

	public static void rangeData(HTable table) throws Exception {

		Scan scan = new Scan();

		// conf the scan
		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
		//scan.setStartRow(Bytes.toBytes("20170521_10002"));
		//scan.setStopRow(Bytes.toBytes("20170521_10003"));
		// load the scan
		ResultScanner rsscan = table.getScanner(scan);
		for (Result rs : rsscan) {
			System.out.println(Bytes.toString(rs.getRow()));
			for (Cell cell : rs.rawCells()) {
				System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + "->"
						+ Bytes.toString(CellUtil.cloneQualifier(cell)) + "->"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "->" + cell.getTimestamp());
			}
			System.out.println("---------------------------------------------");
		}
	}

	public static void main(String[] args) throws Exception {

		HTable table = getTable("t1");
		//getData(table);
		//putData(table);
		//deleteData(table);
		scanData(table);
		//rangeData(table);
	}
}
