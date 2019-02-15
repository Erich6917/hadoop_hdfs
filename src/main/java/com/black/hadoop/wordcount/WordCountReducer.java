package com.black.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Hunter
 * @version 1.0, 21:07 2019/1/27
 *
 * reducer阶段接收的是Mapper输出的数据
 * mapper的输出是reducer输入
 *
 * keyIn:mapper输出的key的类型
 * valueIn:mapper输出value的类型
 *
 * reducer端输出的数据类型，想要一个什么样的结果<hello,1888>
 * keyOut:Text
 * valueOut:IntWritable
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    //key->单词 values->次数 1 1 1 1 1
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //1.记录出现的次数
        int sum = 0;
        for(IntWritable v:values){
            sum += v.get();
        }

        //2.累加求和输出
        context.write(key,new IntWritable(sum));
    }
}
