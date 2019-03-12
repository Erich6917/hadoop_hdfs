package com.black.hadoop.mapreduce.zone.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.black.hadoop.mapreduce.zone.bean.UserInfo;

/**
 * @Author : Erich ErichLee@qq.com
 * @Date : 2019年3月12日
 * @Comment: 按照季度分区
 * 
 */
public class CommonFlowPartitioner extends Partitioner<Text, UserInfo> {

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