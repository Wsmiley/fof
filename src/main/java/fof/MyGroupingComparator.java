package fof;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupingComparator extends WritableComparator {

    public MyGroupingComparator(){
        super(MyKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MyKey akey = (MyKey) a;
        MyKey bkey = (MyKey) b;


        //按照name属性值判断是否是一组数据
        int result = akey.getName().compareTo(bkey.getName());
        return result;
    }
}
