###目录创建

hadoop fs -mkdir -p /mapred/wc/in



###执行机测试
s1 打成jar包
s2 上传至linux目录
s3 执行命令
hadoop jar /opt/moudle/hadoop-2.7.3/diy/hadoop_hdfs-1.0-sort.jar com.black.hadoop.mapreduce.zone.FlowSort/mapred/wc/in /mapred/wc/output

###注意事项
	input目录赢存放需要统计的文本内让那个
	output目录为新生成目录，否则报错



###本地测试

```
java -DP1=C:\Data\hadoop\in  -DP2=C:\Data\hadoop\out com.black.hadoop.wordcounts.WordCount
```



