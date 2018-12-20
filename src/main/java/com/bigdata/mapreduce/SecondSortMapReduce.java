package com.bigdata.mapreduce;

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
public class SecondSortMapReduce extends Configured implements Tool {

    //mapper
    public static class wordCountMapper extends Mapper<LongWritable, Text, PairWritable, IntWritable>{
        private PairWritable mapOutputKey = new PairWritable();
        private IntWritable mapOutputValue = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //每一行的值
            String lineValue = value.toString();
            //split
            StringTokenizer tokenizer = new StringTokenizer(lineValue);
            while (tokenizer.hasMoreTokens()){
            	String[] fields = tokenizer.nextToken().split(",");
            	if(fields.length != 2){
            		return;
            	}
                mapOutputKey.set(fields[0], Integer.valueOf(fields[1]));
                mapOutputValue.set(Integer.valueOf(fields[1]));
                context.write(mapOutputKey, mapOutputValue);
            }
        }
    }

    //reducer
    public static class wordCountReducer extends Reducer<PairWritable, IntWritable, Text, IntWritable>{
        private Text outputKey = new Text();

        @Override
        public void reduce(PairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	for(IntWritable value : values){
        		outputKey.set(key.getFirst());
                context.write(outputKey, value);
        	}
        }
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //获取集群配置
        //Configuration configuration = new Configuration();
        Configuration configuration = this.getConf();

        //准备job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(SecondSortMapReduce.class);
        //设置job

        //input
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inpath);
        //ouput
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);

        //mapper
        job.setMapperClass(wordCountMapper.class);
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //patition and sort and spill to disk
        //分区
        job.setPartitionerClass(FirstPartitoner.class);
        //排序
        job.setSortComparatorClass(FirstGroupComparator.class);
        //优化
        //job.setCombinerClass(cls);
        //分组
        //job.setGroupingComparatorClass(cls);

        //reducer
        job.setReducerClass(wordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //提交job => yarn, 等待job完成
        boolean isComplete = job.waitForCompletion(true);

        return  isComplete ? 0 : 1;
    }

    public static void main(String[] args){
    	
    	args = new String[]{
    			"hdfs://master:8020/secondarysort/data/",
    			"hdfs://master:8020/secondarysort/output01"
    	};

        Configuration conf = new Configuration();
        int status = 0;
        try {
            status = ToolRunner.run(conf, new SecondSortMapReduce(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(status);
    }

}
