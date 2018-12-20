package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestDriver1 extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "mr-hbase");
		job.setJarByClass(TestDriver1.class);
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("stu_info", // input table
				scan, // Scan instance to control CF and attribute selection
				TestHbaseMapper.class, // mapper class
				ImmutableBytesWritable.class, // mapper output key
				Put.class, // mapper output value
				job,
				false);
		TableMapReduceUtil.initTableReducerJob("t9999", // output table
				null, // reducer class
				job,
				null,
				null,
				null,
				null,
				false);
		job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args){
		Configuration conf = HBaseConfiguration.create();
		//conf.set("hbase.nameserver.address", "master.bigdata.com");  
		try {
			int status = ToolRunner.run(conf, new TestDriver1(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
