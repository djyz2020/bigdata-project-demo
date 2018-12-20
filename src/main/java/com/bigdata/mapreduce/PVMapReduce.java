package com.bigdata.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by haibozhang on 2018/2/6.
 */
public class PVMapReduce extends Configured implements Tool {

    //mapper
    public static class wordCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        private IntWritable mapOutputKey = new IntWritable();
        private IntWritable mapOutputValue = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //每一行的值
            String lineValue = value.toString();
            //split
            String[] fields = lineValue.split("\t");
        	if(fields.length < 30){
        		//自定义计数器
        		context.getCounter("webpvmapper", "length_lt_30_count").increment(1);
        		return;
        	}
        	String url = fields[1];
        	String provinceId = fields[23];
        	if(StringUtils.isEmpty(url)){
        		//自定义计数器
        		context.getCounter("webpvmapper", "url_is_empty").increment(1);
            	return;
            }
            if(StringUtils.isEmpty(provinceId)){
            	//自定义计数器
        		context.getCounter("webpvmapper", "provinceId_is_empty").increment(1);
            	return;
            }
            int tmp_provinceId = 0;
            try {
            	tmp_provinceId = Integer.valueOf(provinceId);
			} catch (Exception e) {
				//自定义计数器
        		context.getCounter("webpvmapper", "provinceId_is_not_number").increment(1);
				return;
			}
            mapOutputKey.set(tmp_provinceId);
            context.write(mapOutputKey, mapOutputValue);
        }
    }

    //reducer
    public static class wordCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        private IntWritable outputKey = new IntWritable();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            outputKey.set(sum);
            context.write(key, outputKey);
        }
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //获取集群配置
        //Configuration configuration = new Configuration();
        Configuration configuration = this.getConf();

        //准备job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(PVMapReduce.class);
        //设置job

        //input
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inpath);
        //ouput
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);

        //mapper
        job.setMapperClass(wordCountMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //patition and sort and spill to disk
        //设置分区类
        //job.setPartitionerClass("");
        //设置比较类
        //job.setSortComparatorClass("");
        //设置分组类
        //job.setGroupingComparatorClass("");

        //reducer
        job.setReducerClass(wordCountReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        //设置Reducer的数量
        //job.setNumReduceTasks(3);

        //等待job完成
        boolean isComplete = job.waitForCompletion(true);

        return  isComplete ? 0 : 1;
    }

    public static void main(String[] args){
    	
    	args = new String[] {
				"hdfs://master:8020/pv/data/test_data/2015082818",
				"hdfs://master:8020/pv/data/test_data/2015082818_output02" };

        Configuration conf = new Configuration();
        int status = 0;
        try {
            status = ToolRunner.run(conf, new PVMapReduce(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(status);
    }

}
