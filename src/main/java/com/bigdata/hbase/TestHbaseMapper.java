package com.bigdata.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Mapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT>
 * ImmutableBytesWritable->rowkey Result->Cell
 * 
 * @author ibf
 *
 */
public class TestHbaseMapper extends TableMapper<ImmutableBytesWritable, Put> {

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {

		// 封装put
		Put put = new Put(key.get());
		for (Cell cell : value.rawCells()) {
			// 判断当前cell列簇是否为info
			if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
				// 判断当前cell列簇info的列是否为name
				if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					// 如果上面条件都匹配，就将cell放进去
					put.add(cell);
				}
			}
		}
		if(!put.isEmpty()){ //防止put为空
			context.write(key, put);
		}
	}
}
