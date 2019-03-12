package com.black.hadoop.mapreduce.zone;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.black.hadoop.mapreduce.zone.bean.UserInfo;
import com.black.hadoop.mapreduce.zone.common.CommonFlowMapper;
import com.black.hadoop.mapreduce.zone.common.CommonFlowReducer;
/**
 * @Author : Erich ErichLee@qq.com
 * @Date : 2019年3月4日
 * @Comment: demo1-根据用户信息 统计流量信息-基础版本
 */
public class FlowCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 1.创建job任务
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2.指定jar包位置
		job.setJarByClass(FlowCount.class);

		// 3.关联使用的Mapper类
		job.setMapperClass(CommonFlowMapper.class);

		// 4.关联使用的Reducer类
		job.setReducerClass(CommonFlowReducer.class);

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

		// 7.设置数据输入的路径 默认TextInputFormat
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// 8.设置数据输出的路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 9.提交任务
		boolean rs = job.waitForCompletion(true);
		System.exit(rs ? 0 : 1);
	}

}
