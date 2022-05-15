package fof;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKey implements WritableComparable<MyKey> {
    private String name;
    private String toFriend;
    //共同好友数
    private Integer coFriendNum;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getToFriend() {
        return toFriend;
    }

    public void setToFriend(String toFriend) {
        this.toFriend = toFriend;
    }

    public Integer getCoFriendNum() {
        return coFriendNum;
    }

    public void setCoFriendNum(Integer coFriendNum) {
        this.coFriendNum = coFriendNum;
    }

    @Override
    public int compareTo(MyKey other) {
        int result = this.getName().compareTo(other.getName());
        if(result == 0){
            //如果name相同，则表示同一个人推荐的好友，这些好友要按照共同好友数倒序排列。
            result = other.getCoFriendNum().compareTo(this.getCoFriendNum());
        }

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //序列号写出
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(toFriend);
        dataOutput.writeInt(coFriendNum);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //反序列化
        setName(dataInput.readUTF());
        setToFriend(dataInput.readUTF());
        setCoFriendNum(dataInput.readInt());
    }
}