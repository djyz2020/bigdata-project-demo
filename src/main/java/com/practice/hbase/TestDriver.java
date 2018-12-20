package com.practice.hbase;

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
import org.apache.log4j.BasicConfigurator;

public class TestDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "mr-hbase");
		job.setJarByClass(TestDriver.class);
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob(
				"ns1:tb1",
				scan, 
				TestHbaseMapper.class, 
				ImmutableBytesWritable.class, 
				Put.class, 
				job);
		TableMapReduceUtil.initTableReducerJob(
				"20180401", 
				null, 
				job);
		
		job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : 1 ;
	}
	
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		//conf.set("hbase.nameserver.address", "master.bigdata.com");
		try {
			int status = ToolRunner.run(conf, new TestDriver(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
