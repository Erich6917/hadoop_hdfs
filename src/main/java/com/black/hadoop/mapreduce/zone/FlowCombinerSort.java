package com.black.hadoop.mapreduce.zone;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.black.hadoop.mapreduce.zone.bean.UserInfo;
import com.black.hadoop.mapreduce.zone.common.CommonFlowMapper;

/**
 * @Author : Erich ErichLee@qq.com
 * @Date : 2019年3月13日
 * @Comment : combiner为 map 和 reducer 中间过程 in : map.out out: reducer.in
 */
public class FlowCombinerSort {
	public static void main(String[] args)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowCombinerSort.class);

		job.setMapperClass(CommonFlowMapper.class);
		job.setCombinerClass(FlowCombinerProcess.class);
		job.setReducerClass(FlowReducerProcess.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserInfo.class);

		job.setOutputKeyClass(UserInfo.class);
		job.setOutputValueClass(Text.class);

		// 设置读取数据切片的类
		job.setInputFormatClass(CombineTextInputFormat.class);
		// 最大切片大小8M
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

	public static class FlowCombinerProcess extends Reducer<Text, UserInfo, Text, UserInfo> {
		@Override
		protected void reduce(Text key, Iterable<UserInfo> values, Context context)
				throws IOException, InterruptedException {
			long flowUp = 0;
			long flowDown = 0;
			Iterator<UserInfo> cpVals = values.iterator();
			UserInfo firstUser = cpVals.next();

			for (UserInfo info : values) {
				flowUp += info.getFlowUp();
				flowDown += info.getFlowDown();
			}
			// 获取第一个bean的基础信息作为公共信息

			UserInfo useInfo = new UserInfo(firstUser.getName(), firstUser.getId(), "ALL", flowUp, flowDown);
//			context.write(useInfo, key);
			context.write(key, useInfo);
		}

	}

	public static class FlowReducerProcess extends Reducer<Text, UserInfo, UserInfo, Text> {

		@Override
		protected void reduce(Text key, Iterable<UserInfo> values, Context context)
				throws IOException, InterruptedException {
			context.write(values.iterator().next(), key);
		}

	}
}
