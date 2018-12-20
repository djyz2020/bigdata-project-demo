package com.bigdata.hive01;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class DistributedCacheWCMapReduce extends Configured implements Tool{
	public static Configuration conf = new Configuration();

	//mapper
	public static class DistributedMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text mapOutputKey = new Text();
		private Text mapOutputValue = new Text();
		private List<String> list = new ArrayList<String>();
		private Map<String, String> dataMap = new HashMap<String, String>();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//1. get configuration
			
			//2. get cache uri
			URI uris[] = DistributedCache.getCacheFiles(conf);
			//3. path
			Path path = new Path(uris[0]);
			//System.out.println(path);
			//Path path = new Path("hdfs://master:8020/dataJoinMapreduce/cache/customers.csv");
			//4. file system
			FileSystem fs = FileSystem.get(conf);
			//5. in stream
			InputStream in = fs.open(path);
			//6. read data
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			while((line = br.readLine()) != null){
				if(line != null && !"".equals(line.trim())){
					String[] strs = line.split(",");
					String userId = strs[0];
					String name = strs[1];
					String tel = strs[2];
					list.add(userId);
					dataMap.put(userId, userId + "\t" + name + "\t" + tel);
				}
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,
				Text, Text>.Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			StringTokenizer st = new StringTokenizer(lineValue);
			while(st.hasMoreTokens()){
				String record = st.nextToken();
				if(record == null){
					continue;
				}
				String[] strs = record.split(",");
				String userId = strs[0];
				String recordId = strs[1];
				String price = strs[2];
				String cDate = strs[3];
				if(!list.contains(userId)){
					continue;
				}
				// output
				mapOutputKey.set(userId);
				mapOutputValue.set(dataMap.get(userId) + "\t" + recordId + "\t" + price + "\t" + cDate);
				context.write(mapOutputKey, mapOutputValue);
			}
		}
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
	
	//reducer
	public static class DistributedReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
						throws IOException, InterruptedException {
			// iterator
			for(Text value : values){
				// job output 
				context.write(null, value);
			}
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
		
	}
	
	public int run(String[] args) throws Exception{
		
		//1. get configuration
		//Configuration conf = new Configuration();
		Configuration conf = this.getConf();
		
		//2. create job
		Job job = this.parseInputAndOutput(this, conf, args);
		
		//3. set job
		//mapper class
		job.setMapperClass(DistributedMapper.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//reducer class
		job.setReducerClass(DistributedReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//submit job
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1;
		
	}
	
	public Job parseInputAndOutput(Tool tool,Configuration conf,String[] args) throws Exception {
		
		// validate args
		if(args.length != 2){
			System.err.println("Usage : " + tool.getClass().getSimpleName() + " [generic options] <input> <output>");
			ToolRunner.printGenericCommandUsage(System.err);
			return null;
		}
		
		// step 2:create job
		Job job = Job.getInstance(conf, tool.getClass().getSimpleName());
		
//		job.addCacheFile(uri);
		
		// step 3:set job run class
		job.setJarByClass(tool.getClass());
		
		// 1:input format
		Path inputPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputPath);
		
		// 5:output format
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		args = new String[]{
				"hdfs://master:8020/dataJoinMapreduce/input",
				"hdfs://master:8020/dataJoinMapreduce/output02"
		};
		
		// mapreduce-default.xml,mapreduce-site.xml
		//Configuration conf = new Configuration();
		
		// set distributed cache
		// ===============================================================
		URI uri = new URI("hdfs://master:8020/dataJoinMapreduce/cache/customers.csv");
		DistributedCache.addCacheFile(uri, conf);
		// ===============================================================
		
		// run mapreduce
		int status = ToolRunner.run(conf, new DistributedCacheWCMapReduce(), args);
		
		// exit program
		System.exit(status);
	}
	
}



