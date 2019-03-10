package com.black.hadoop.mapreduce.zone;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Author : Erich ErichLee@qq.com
 * @Date : 2019年3月8日
 * @Comment: demo4-防止数据倾斜，文件合并后执行
 * 
 */
public class FlowCombiner {
	public static void main(String[] args) {

	}

	public static class FlowMapper extends Mapper<LongWritable, Text, Text, UserInfo> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// read msg
			String text = value.toString();
			String[] lines = text.split("\n");

			for (String line : lines) {
				String[] fields = line.split("\t");
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
				if (demo.getId() == null) {
					demo.setName(info.getName());
					demo.setId(info.getId());
					demo.setDate("2018");
				}
			}

			UserInfo useInfo = new UserInfo(demo.getName(), demo.getId(), demo.getDate(), flowUp, flowDown);
			context.write(key, useInfo);
		}

	}

}
