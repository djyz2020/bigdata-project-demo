package com.bigdata.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MapReduceModule {
	
	//1.mapper class
	public static class MapReduceMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text mapOutputKey = new Text();
		private IntWritable mapOutputValue = new IntWritable(1);
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// line value
			String lineValue = value.toString();

			// spilt
			// String[] strs = lineValue.split(" ");
			StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
			while (stringTokenizer.hasMoreTokens()) {
				// set map output key
				mapOutputKey.set(stringTokenizer.nextToken());
				// output
				context.write(mapOutputKey, mapOutputValue);
			}
			
			/**
			 * // iterator for (String str : strs) {
			 * 
			 * mapOutputKey.set(str);
			 * 
			 * context.write(mapOutputKey, mapOutputValue);
			 * 
			 * }
			 */
			
		}
	}
	
	//2.reducer class 
	public static class MapReduceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable outputValue = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// temp sum
			int sum = 0;

			// iterator
			for (IntWritable value : values) {
				sum += value.get();
			}

			// set output
			outputValue.set(sum);

			context.write(key, outputValue);
		}
	}
	
	//3.driver
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		//获取集群中的相关配置信息
		Configuration configuration = new Configuration();
		//创建一个job任务
		Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
		//整个MapReduce程序运行的入口，或者叫jar包的入口，jar具体运行的是哪个类
		job.setJarByClass(this.getClass());
		
		//设置Job
		//input输入,输入路径
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);
		//output输出，输出路径
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outpath);
		
		//设置Mapper
		job.setMapperClass(MapReduceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置Reducer
		job.setReducerClass(MapReduceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//提交job => YARN
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1;
	}
	
	//主方法
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] {
				"hdfs://slave01:8020/df/data/wordCount.txt",
				"hdfs://slave01:8020/df/data/output11" };

		//run job
		int status = new MapReduceModule().run(args);
		//close
		System.exit(status);
	}

}


