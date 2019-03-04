###目录创建

hadoop fs -mkdir -p /mapred/zone/in



###执行机测试

hadoop jar /opt/moudle/hadoop-2.7.3/diy/hadoop_hdfs-1.0-sort.jar 

com.black.hadoop.mapreduce.zone.FlowSort /mapred/zone/in /mapred/zone/out