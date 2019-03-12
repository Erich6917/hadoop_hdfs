package com.black.hadoop.mapreduce.zone.common;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.black.hadoop.mapreduce.zone.bean.UserInfo;

/**
 * @Author : Erich ErichLee@qq.com
 * @Date : 2019年3月12日
 * @Comment: 格式输入 text - bean 格式输出 text - bean
 * 
 */
public class CommonFlowReducer extends Reducer<Text, UserInfo, Text, UserInfo> {
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
		context.write(key, useInfo);
	}
}
