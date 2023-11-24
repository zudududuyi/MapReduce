package com.YTao.mapreduce.Big_Project.Bpartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String province = flowBean.getProvince();

        //定义一个分区
        if("GuangDong".equals(province)){
            i=0;
        } else if ("GuangXi".equals(province)) {
            i=1;
        }else if ("FuJian".equals(province)) {
            i=2;
        }else if ("HuNan".equals(province)) {
            i=3;
        }else{
            i=4;
        }

        return i;
    }
}
