package fof;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FOFReducer1 extends Reducer<MyKey, Text,Text, NullWritable> {
    @Override
    protected void reduce(MyKey key, Iterable<Text> values, Reducer<MyKey, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        int i = 2;

        for (Text value:values){
            if (i>0){
                context.write(value,NullWritable.get());
                i--;
            }else {
                break;
            }

        }
//        values.forEach(value->{
//            try {
//                context.write(value,NullWritable.get());
//            }catch (IOException e){
//                e.printStackTrace();
//            }catch (InterruptedException e){
//                e.printStackTrace();
//            }
//        });
    }
}
