package fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MainClass {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args ==null || args.length!=3){
            System.out.println("useage:yarn jar myfof.jar <input> <output>");
            System.exit(1);
        }
        //加载配置文件，包括默认的配置文件
        Configuration configuration = new Configuration(true);

        Job job1 = Job.getInstance(configuration);
        job1.setJobName("fof");
        job1.setJarByClass(MainClass.class);

        //设置输入输出路径
        FileInputFormat.addInputPath(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]));

        //设置mapper
        job1.setMapperClass(FOFMapper.class);
        //设置reduce
        job1.setReducerClass(FOFReducer.class);
        //设置map输出key的类型
        job1.setMapOutputKeyClass(Text.class);
        //设置map输出value的类型
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class);

        if (job1.waitForCompletion(true)) {
            Configuration configuration1 = new Configuration(true);
            Job job2 = Job.getInstance(configuration1);
            job2.setJarByClass(MainClass.class);
            job2.setMapperClass(FOFMapper1.class);
            job2.setReducerClass(FOFReducer1.class);

            job2.setMapOutputKeyClass(MyKey.class);
            job2.setMapOutputValueClass(Text.class);
            //设置inputformat的具体实现，key是行中第一个\t之前的部分，如果没有\t,则整行是key,value是空
            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            //设置分组比较器
            job2.setGroupingComparatorClass(MyGroupingComparator.class);

            //设置分区器
            job2.setPartitionerClass(MyPartitioner.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            System.exit(job2.waitForCompletion(true)? 0 : 1);

        }
        job1.waitForCompletion(true);
    }
}
