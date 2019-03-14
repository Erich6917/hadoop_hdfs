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
 * @Comment: 求出每门课程参考学生成绩最高的学生的信息：课程，姓名和平均分。
 * 
 */
public class CourseC {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(CourseC.class);

		job.setMapperClass(CourseMapper.class);
		job.setReducerClass(CourseReducer.class);

		job.setMapOutputKeyClass(CourseBean2.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(CourseBean2.class);
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

	public static class CourseMapper extends Mapper<LongWritable, Text, CourseBean2, NullWritable> {

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
			long scoreMax = -1;
			for (int i = 2; i < msg_arr.length; i++) {
				long currScore = Long.valueOf(msg_arr[i]).longValue();
				scoreTotal += currScore;
				if (currScore > scoreMax) {
					scoreMax=currScore;
				}
			}

			float courseAvg = (float) (Math.round(1.0f * scoreTotal / scoreTimes * 10) / 10);
			CourseBean2 cBean = new CourseBean2(courseName, studentName, courseAvg,scoreMax);
			context.write(cBean, NullWritable.get());
		}

	}

	public static class CourseReducer extends Reducer<CourseBean2, NullWritable, CourseBean2, NullWritable> {

		@Override
		protected void reduce(CourseBean2 cBean, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(cBean, NullWritable.get());
		}

	}

	public static class CoursePartitioner extends Partitioner<CourseBean2, NullWritable> {

		@Override
		public int getPartition(CourseBean2 key, NullWritable value, int numPartitions) {
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
