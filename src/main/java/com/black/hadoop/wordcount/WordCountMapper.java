package com.black.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Hunter
 * @version 1.0, 20:53 2019/1/27
 *
 * 海量数据
 *
 * helloitstar
 * mimi
 *
 * 数据的输入与输出以Key value进行传输
 * keyIN:LongWritable(long)  数据的起始偏移量
 * valueIN:具体数据 Text
 *
 * mapper需要把数据传递到reducer阶段(<hello,1>)
 * keyOut：单词 Text
 * valueOut：出现的次数 IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    //对数据进行打散
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.接入数据 hello reba hello mimi
        String line = value.toString();

        //2.对数据进行切分
        String[] words = line.split(" ");

        //3.写出以<hello,1>
        for(String w:words){
            //写出reducer端
            context.write(new Text(w),new IntWritable(1));
        }
    }
}
