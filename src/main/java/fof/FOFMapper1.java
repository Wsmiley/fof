package fof;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FOFMapper1 extends Mapper<Text, Text,MyKey,Text> {
    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, MyKey, Text>.Context context) throws IOException, InterruptedException {
        //第一个mapper的输出作为输入

        //输入cat_hadoop      2  有个分割符号\t
        //key:cat_hadoop  value:2

        String keyStr = key.toString();
        String[] nameParts = keyStr.split("_");

        MyKey myKey = new MyKey();
        myKey.setName(nameParts[0]);
        myKey.setToFriend(nameParts[1]);
        myKey.setCoFriendNum(Integer.valueOf(value.toString()));

        //<obj,"cat_hoadoop\t2">

        context.write(myKey,new Text(nameParts[0]+"_"+nameParts[1]+"\t"+value.toString()));

        myKey.setName(nameParts[1]);
        myKey.setToFriend(nameParts[0]);
        //<obj,"hadoop_cat\t2">
        context.write(myKey,new Text(nameParts[1]+"_"+nameParts[0]+"\t"+value.toString()));
    }
}
