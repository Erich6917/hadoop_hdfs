package com.black.hadoop.mapreduce.workbook.demo01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author : Erich ErichLee@qq.com
 * @Date : 2019年3月14日
 * @Comment: 统计每门课程参考学生的平均分，并且按课程存入不同的结果文件， 要求一门课程一个结果文件，并且按平均分从高到低排序，分数保留一位小数。
 * 
 */
public class CourseB {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(CourseB.class);

		job.setMapperClass(CourseMapper.class);
		job.setReducerClass(CourseReducer.class);

		job.setMapOutputKeyClass(CourseBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(CourseBean.class);
		job.setOutputValueClass(NullWritable.class);

//		设置自定义分区类，并指明分区个数
		job.setPartitionerClass(CoursePartitioner.class);
		job.setNumReduceTasks(4);

		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, 50 * 1024 * 1024);
		CombineTextInputFormat.setMinInputSplitSize(job, 6 * 1024 * 1024);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 9.提交任务
		boolean rs = job.waitForCompletion(true);
		System.exit(rs ? 0 : 1);

	}

	public static class CourseMapper extends Mapper<LongWritable, Text, CourseBean, NullWritable> {

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

			float courseAvg = (float) (Math.round(1.0f * scoreTotal / scoreTimes * 10) / 10);
			CourseBean cBean = new CourseBean(courseName, studentName, courseAvg);
			context.write(cBean, NullWritable.get());
		}

	}

	public static class CourseReducer extends Reducer<CourseBean, NullWritable, CourseBean, NullWritable> {

		@Override
		protected void reduce(CourseBean cBean, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(cBean, NullWritable.get());
		}

	}

	public static class CoursePartitioner extends Partitioner<CourseBean, NullWritable> {

		@Override
		public int getPartition(CourseBean key, NullWritable value, int numPartitions) {
			String courseName = key.getCourseName();
			if (courseName.contains("english")) {
				return 1;
			} else if (courseName.contains("algorithm")) {
				return 2;
			} else if (courseName.contains("math")) {
				return 3;
			}
			return 0;

		}

	}
}
