### MR练习题1-学生成绩相关题目

​	链接 https://blog.csdn.net/zyz_home/article/details/79937228



1、统计每门课程的参考人数和课程平均分

2、统计每门课程参考学生的平均分，并且按课程存入不同的结果文件，要求一门课程一个结果文件，并且按平均分从高到低排序，分数保留一位小数。

3、求出每门课程参考学生成绩最高的学生的信息：课程，姓名和平均分。

数据及字段说明：

​	computer,huangxiaoming,85,86,41,75,93,42,85

​	english,zhaobenshan,54,52,86,91,42,85,75

数据解释
数据字段个数不固定：
第一个是课程名称，总共四个课程，computer，math，english，algorithm，
第二个是学生姓名，后面是每次考试的分数，但是每个学生在某门课程中的考试次数不固定。

### Course1-CourseA

```
hadoop jar /opt/moudle/hadoop-2.7.3/diy/hadoop_hdfs-2.0-zone.jar com.black.hadoop.mapreduce.workbook.demo01.CourseA /mapred/course/in /mapred/course/out1
```

