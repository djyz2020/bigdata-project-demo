package com.practice.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Mapper;

public class TestHbaseMapper extends TableMapper<ImmutableBytesWritable, Put> {
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
					throws IOException, InterruptedException {
		Put put = new Put(key.get()); //获取行
		List<Cell> list = value.listCells();
		for(Cell cell : list){
			if("f1".equals(CellUtil.cloneFamily(cell))){
				if("name".equals(CellUtil.cloneQualifier(cell))){
					put.add("info".getBytes(), "name".getBytes(), CellUtil.cloneValue(cell));
				}
			}
		}
		context.write(key, put);
	}

}
