package fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FOFReducer extends Reducer<Text,Text,Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //<"hello_hadoop","R">
        //<"hello_cat","G">

        int sum = 0;
        for(Text value:values){
            String val = value.toString();
            if("R".equals(val)){
                //R表示已经认识直接忽略
                return;
            }
            sum++;
        }
        //<"hadoop_hello", 3>   hadoop与hello 之间有3个共同好友
        context.write(key,new IntWritable(sum));
    }

}
