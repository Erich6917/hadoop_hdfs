###目录创建

hadoop fs -mkdir -p /mapred/zone/in



###执行机测试-基础版本FlowCount

hadoop jar /opt/moudle/hadoop-2.7.3/diy/hadoop_hdfs-1.0-count.jar 

com.black.hadoop.mapreduce.zone.FlowSort /mapred/zone/in /mapred/zone/count


###执行机测试-季度分区FlowPartition



###执行机测试-依照时间排序

