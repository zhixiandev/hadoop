package com;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	public static class WCMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
		private final static LongWritable lw = new LongWritable();
		private Text word = new Text();
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line, "\t\r\n\f|,.()<>");
			while(st.hasMoreTokens()) {
				context.write(word,lw);
			}
		}
	}
	public static class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		private LongWritable lw = new LongWritable(1);
		
		protected void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
			long sum = 0;
			for(LongWritable value:values) {
				sum += value.get();
				
			}
			lw.set(sum);
			context.write(key,lw);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordCount");
		job.setJarByClass(WordCount.class); 
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job,new Path("word.txt"));
		FileOutputFormat.setOutputPath(job, new Path("word.log"));
		job.waitForCompletion(true);
	}
	
	
}