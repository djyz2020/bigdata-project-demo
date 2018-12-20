package com.bigdata.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHBaseMapper1 extends TableMapper<ImmutableBytesWritable, Put>{
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
					throws IOException, InterruptedException {
			//封装cell
			Put put = new Put(key.get());
			for(Cell cell : value.rawCells()){
				if("f1".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
					if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
						put.add(cell);
					}
				}
			}
			if(!put.isEmpty()){
				context.write(key, put);
			}
	}
}


