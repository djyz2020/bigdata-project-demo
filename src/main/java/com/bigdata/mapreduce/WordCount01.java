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
public class WordCount01 extends Configured implements Tool {

    //mapper
    public static class wordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text mapOutputKey = new Text();
        private IntWritable mapOutputValue = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //每一行的值
            String lineValue = value.toString();
            //split
            StringTokenizer tokenizer = new StringTokenizer(lineValue);
            while (tokenizer.hasMoreTokens()){
                mapOutputKey.set(tokenizer.nextToken());
                context.write(mapOutputKey, mapOutputValue);
            }
        }
    }

    //reducer
    public static class wordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable outputKey = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
        job.setJarByClass(WordCount01.class);
        //设置job

        //input
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inpath);
        //ouput
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);

        //mapper
        job.setMapperClass(wordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
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
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //等待job完成
        boolean isComplete = job.waitForCompletion(true);

        return  isComplete ? 0 : 1;
    }

    public static void main(String[] args){

        Configuration conf = new Configuration();
        int status = 0;
        try {
            status = ToolRunner.run(conf, new WordCount01(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(status);
    }

}
