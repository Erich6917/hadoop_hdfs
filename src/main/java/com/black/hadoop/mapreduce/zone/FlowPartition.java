package com.black.hadoop.mapreduce.zone;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author : Erich ErichLee@qq.com
 * @Date : 2019年3月4日
 * @Comment: 根据用户信息 统计流量信息-增加季度分区功能
 * 
 */
public class FlowPartition {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 1.创建job任务
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2.指定jar包位置
		job.setJarByClass(FlowPartition.class);

		// 3.关联使用的Mapper类
		job.setMapperClass(FlowMapper.class);

		// 4.关联使用的Reducer类
		job.setReducerClass(FlowReducer.class);

		// 5.设置mapper阶段输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserInfo.class);

		// 6.设置reducer阶段输出的数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(UserInfo.class);

//        //设置读取数据切片的类
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        //最大切片大小8M
//        CombineTextInputFormat.setMaxInputSplitSize(job,8388608);
//        //最小切片大小6M
//        CombineTextInputFormat.setMinInputSplitSize(job,6291456);

		// 加入自定义分区
		job.setPartitionerClass(FlowShowPartitioner.class);
		// 注意：结果文件几个？
		job.setNumReduceTasks(4);

		// 7.设置数据输入的路径 默认TextInputFormat
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// 8.设置数据输出的路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 9.提交任务
		boolean rs = job.waitForCompletion(true);
		System.exit(rs ? 0 : 1);
	}

	public static class FlowMapper extends Mapper<LongWritable, Text, Text, UserInfo> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// read msg
			String text = value.toString();
			String[] lines = text.split("\t\n");

			for (String line : lines) {
				String[] fields = line.split(" ");
				// parse msg
				String name = fields[0];
				String id = fields[1];
				String date = fields[2];
				long flowUp = Long.parseLong(fields[3]);
				long flowdown = Long.parseLong(fields[4]);

				UserInfo userInfo = new UserInfo(name, id, date, flowUp, flowdown);

				// write msg
				context.write(new Text(id), userInfo);
			}
		}

	}

	public static class FlowReducer extends Reducer<Text, UserInfo, Text, UserInfo> {

		@Override
		protected void reduce(Text key, Iterable<UserInfo> values, Context context)
				throws IOException, InterruptedException {
			long flowUp = 0;
			long flowDown = 0;
			UserInfo demo = new UserInfo();

			for (UserInfo info : values) {
				flowUp += info.getFlowUp();
				flowDown += info.getFlowDown();
				if (demo != null) {
					demo.setName(info.getName());
					demo.setId(info.getId());
					demo.setDate(info.getDate());
				}
			}
//			UserInfo demo = values.iterator().next();

			UserInfo useInfo = new UserInfo(demo.getName(), demo.getId(), demo.getDate(), flowUp, flowDown);
			context.write(key, useInfo);
		}

	}

}

class FlowShowPartitioner extends Partitioner<Text, UserInfo> {

	@Override
	public int getPartition(Text key, UserInfo value, int numPartitions) {
		// 根据时间
		String date = value.getDate();

		if ("201801".equals(date) || "201802".equals(date) || "201803".equals(date)) {
			return 1;
		} else if ("201804".equals(date) || "201805".equals(date) || "201806".equals(date)) {
			return 2;
		} else if ("201807".equals(date) || "201808".equals(date) || "201809".equals(date)) {
			return 3;
		}

		return 0;
	}

}
