package fof;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FOFMapper extends Mapper<LongWritable,Text,Text,Text>{
    //重写map方法
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //获取文件第一行 tom hello hadoop cat
        String line = value.toString();
        //{"tom","heLLo","hadoop","cat"}
        String[] names = line.split(" ");

        //两两组合
        for (int i = 0; i < names.length-1; i++){
            for (int j = i + 1; j < names.length; j++) {
                if (i == 0){
                    //R表示认识
                    context.write(new Text(getNames(names[i],names[j])) , new Text("R"));
                }else {
                    //G表示有共同好友
                    context.write(new Text(getNames(names[i],names[j])),new Text("G"));
                }
            }
        }
    }

    private  String getNames(String namea,String nameb){
        int result = namea.compareTo(nameb);
        if (result < 0){
            return namea +"_" + nameb;
        }
        return nameb +"_"+ namea;
    }
}
