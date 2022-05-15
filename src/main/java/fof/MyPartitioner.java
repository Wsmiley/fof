package fof;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyKey, Text> {
    @Override
    public int getPartition(MyKey myKey, Text text, int numPartitions) {
        //1.相同name的肯定在一个reducer中，不同name值随机分散在所有的reducer中
        return (myKey.getName().hashCode()&Integer.MAX_VALUE) % numPartitions;

    }
}
