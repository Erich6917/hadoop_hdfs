###执行步骤
s1 打成jar包
s2 上传至linux目录
s3 执行命令
hadoop jar hadoop_hdfs-1.0-SNAPSHOT.jar com.black.hadoop.wordcounts.WordCount /wc/input/ /wc/output3

###注意事项
	input目录赢存放需要统计的文本内让那个
	output目录为新生成目录，否则报错