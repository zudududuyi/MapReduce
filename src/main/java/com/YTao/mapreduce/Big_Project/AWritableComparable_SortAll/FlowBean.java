package com.YTao.mapreduce.Big_Project.AWritableComparable_SortAll;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private int chinese;        //六个科目，各科
    private int math;
    private int english;
    private int politic;
    private int biology;
    private int physics;
    private int sum;            //总成绩
    private String province;    //省份

    //2 提供无参构造
    public FlowBean(){

    }

    //3 提供八个参数的getter和setter方法
    public int getChinese() {
        return chinese;
    }

    public void setChinese(int chinese) {
        this.chinese = chinese;
    }

    public int getMath() {
        return math;
    }

    public void setMath(int math) {
        this.math = math;
    }

    public int getEnglish() {
        return english;
    }

    public void setEnglish(int english) {
        this.english = english;
    }

    public int getPolitic() {
        return politic;
    }

    public void setPolitic(int politic) {
        this.politic = politic;
    }

    public int getBiology() {
        return biology;
    }

    public void setBiology(int biology) {
        this.biology = biology;
    }

    public int getPhysics() {
        return physics;
    }

    public void setPhysics(int physics) {
        this.physics = physics;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(chinese);
        dataOutput.writeInt(math);
        dataOutput.writeInt(english);
        dataOutput.writeInt(politic);
        dataOutput.writeInt(biology);
        dataOutput.writeInt(physics);
        dataOutput.writeInt(sum);                  //总分
        dataOutput.writeBytes(province);        //省份

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.chinese = dataInput.readInt();
        this.math = dataInput.readInt();
        this.english = dataInput.readInt();
        this.politic = dataInput.readInt();
        this.biology = dataInput.readInt();
        this.physics = dataInput.readInt();
        this.sum = dataInput.readInt();
        this.province = dataInput.readLine();
    }

    @Override
    public String toString() {
        return chinese + "\t" + math + "\t" + english + "\t" + politic +
                "\t" + biology + "\t" + physics + "\t" + sum + "\t" + province;
    }

    @Override
    public int compareTo(FlowBean o) {
        //按照比较,倒序排列
        if(this.sum > o.sum){
            return -1;
        }else if(this.sum < o.sum){
            return 1;
        }else {
            if(this.chinese > o.chinese){
                return -1;
            }else if(this.chinese < o.chinese){
                return 1;
            }else{
                if(this.math > o.math){
                    return -1;
                }else if(this.math < o.math){
                    return 1;
                }else{
                    return 0;
                }
            }
        }
    }
}
