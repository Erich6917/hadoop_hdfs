###目录创建

hadoop fs -mkdir -p /mapred/zone/in



###执行机测试-基础版本FlowCount

hadoop jar /opt/moudle/hadoop-2.7.3/diy/hadoop_hdfs-1.0-count.jar 

com.black.hadoop.mapreduce.zone.FlowSort /mapred/zone/in /mapred/zone/count


###执行机测试-季度分区FlowPartition



###执行机测试-增加流量大小排序

hadoop jar /opt/moudle/hadoop-2.7.3/diy/hadoop_hdfs-2.0-zone.jar com.black.hadoop.mapreduce.zone.FlowSort /mapred/zone/in /mapred/zone/count

