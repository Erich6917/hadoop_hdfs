package com.black.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

public class HdfsCreateDemo {

	public void ls() throws IOException {

		System.setProperty("HADOOP_USER_NAME", "root");
		// step 1配置参数，指定namenode地址

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.100.120:9000");
		// step 2 创建客户端
		FileSystem client = FileSystem.get(conf);
		// step 3 创建目录
		client.mkdirs(new Path("/sp"));
		client.close();
		System.out.println("Successful");
	}

	public static void main(String[] args) throws IOException {
		HdfsCreateDemo demo = new HdfsCreateDemo();

		demo.ls();
	}
}
