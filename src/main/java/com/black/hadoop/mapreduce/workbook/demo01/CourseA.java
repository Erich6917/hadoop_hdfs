package com.black.hadoop.mapreduce.workbook.demo01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author : Erich ErichLee@qq.com
 * @Date : 2019年3月14日
 * @Comment: 统计每门课程的参考人数和课程平均分
 * 
 */
public class CourseA {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException  {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(CourseA.class);

		job.setMapperClass(CourseMapper.class);
		job.setReducerClass(CourseReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(CombineTextInputFormat.class);
		// 最大切片大小50M
		CombineTextInputFormat.setMaxInputSplitSize(job, 50 * 1024 * 1024);
		// 最小切片大小6M
		CombineTextInputFormat.setMinInputSplitSize(job, 6 * 1024 * 1024);

		// 7.设置数据输入的路径 默认TextInputFormat
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// 8.设置数据输出的路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 9.提交任务
		boolean rs = job.waitForCompletion(true);
		System.exit(rs ? 0 : 1);

	}

	public static class CourseMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String lines = value.toString();
			String[] msg_arr = lines.split("\t");

//			校验数据格式，computer,xuzheng,54,52,86,91,42
			if (msg_arr.length <= 2) {
				return;
			}

			String courseName = msg_arr[0];
			String studentName = msg_arr[1];

			long scoreTotal = 0;
			long scoreTimes = msg_arr.length - 2;
			for (int i = 2; i < msg_arr.length; i++) {
				scoreTotal += Long.valueOf(msg_arr[i]).longValue();
			}

			String rtMsg = String.format("%s\t%s\t%s", studentName, scoreTimes, scoreTotal);
			context.write(new Text(courseName), new Text(rtMsg));
		}

	}

	public static class CourseReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long score_times = 0;
			long score_total = 0;
			long student_count = 0;
			for (Text lines : values) {
				String[] line_arr = lines.toString().split("\t");
				score_times += Long.parseLong(line_arr[1]);
				score_total += Long.parseLong(line_arr[2]);
				student_count += 1;
			}
			long score_ave = score_total / score_times;
			String rt_msg = String.format("%s\t%s", student_count, score_ave);
			context.write(key, new Text(rt_msg));
		}

	}

}
