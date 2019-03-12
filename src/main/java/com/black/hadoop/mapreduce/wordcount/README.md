### 目录创建

- hadoop fs -mkdir -p /mapred/wc/in

### 测试数据

- 测试数据为文本，可任意编写以**\t**为间隔符的字幕组合，最好包含重复和非重复的数字

### 执行机测试

- s1 打成jar包
- s2 上传至linux目录
- s3 执行命令

```
hadoop jar /opt/moudle/hadoop-2.7.3/diy/hadoop_hdfs-1.0-sort.jar com.black.hadoop.mapreduce.zone.FlowSort/mapred/wc/in /mapred/wc/output
```



### 注意事项

- output目录如果存在，则报错。为了防止数据覆盖。





