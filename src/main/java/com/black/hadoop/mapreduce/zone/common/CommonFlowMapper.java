package com.black.hadoop.mapreduce.zone.common;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.black.hadoop.mapreduce.zone.bean.UserInfo;

/**
 * @Author : Erich ErichLee@qq.com
 * @Date : 2019年3月12日
 * @Comment: 格式输出 text - bean
 * 
 */
public class CommonFlowMapper extends Mapper<LongWritable, Text, Text, UserInfo> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// read msg
		String text = value.toString();

		String[] fields = text.split("\t");
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
