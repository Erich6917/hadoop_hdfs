package com.black.hadoop.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author : Erich ErichLee@qq.com
 * @Date : 2019年3月1日
 * @Comment: hadoop-wordcount手写案例
 * 
 */
public class WordCount {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// 1.创建job任务
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2.指定jar包位置
		job.setJarByClass(WordCount.class);

		// 3.关联使用的Mapper类
		job.setMapperClass(WordCountMapper.class);
		// 4.关联使用的Reducer类
		job.setReducerClass(WordCountReducer.class);
		// 5.设置mapper阶段输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 6.设置reducer阶段输出的数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 7.设置数据输入的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 8.设置数据输出的路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 9.提交任务

		boolean rs = job.waitForCompletion(true);
		System.exit(rs ? 0 : 1);
	}

	/**
	 * @Author : Erich ErichLee@qq.com
	 * @Date : 2019年3月1日
	 * @Comment: in-key index in-val 切分单词组 out-key 单词 out-val 单词个数
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// s1 获取内容
			String msg = value.toString();
			// s2 切分
//			String word_arr[] = msg.split(" \t\n\r\f");
			String word_arr[] = msg.split("[ \t\n\r\f]+"); 
			// s3 写入
			for (String word : word_arr) {
				context.write(new Text(word), one);
			}
		}

	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text text, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			context.write(text, new IntWritable(count));

		}

	}
}
