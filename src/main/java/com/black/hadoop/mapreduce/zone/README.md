### 目录创建

hadoop fs -mkdir -p /mapred/zone/in



### MR测试-基础版本FlowCount


```
hadoop jar /opt/moudle/hadoop-2.7.3/diy/hadoop_hdfs-2.0-zone.jar com.black.hadoop.mapreduce.zone.FlowCount /mapred/zone/in /mapred/zone/out1
```

### MR测试-季度分区FlowPartition



```
hadoop jar /opt/moudle/hadoop-2.7.3/diy/hadoop_hdfs-2.0-zone.jar com.black.hadoop.mapreduce.zone.FlowPartition /mapred/zone/in /mapred/zone/out2
```



### 执行机测试-增加流量大小排序 FlowSort

```
hadoop jar /opt/moudle/hadoop-2.7.3/diy/hadoop_hdfs-2.0-zone.jar com.black.hadoop.mapreduce.zone.FlowSort /mapred/zone/in /mapred/zone/out3
```

- 说明：原始输入文本为三个小文件，默认情况下，会开启三个task任务

  ​	为防止数据倾斜，可以使用Combine，设置最大最小切片，先将小文件合并一个大文件

  ​	可设置大文件的大小范围，减少task启动的个数，增加运算效率





