package com.YTao.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //获取手机号前三位 prePhone
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);

        //定义一个分区号变量 partition,根据 prePhone 设置分区号
        int partition;
        if("198".equals(prePhone)){
            partition = 0;
        }else if("133".equals(prePhone)){
            partition = 1;
        }else if("134".equals(prePhone)){
            partition = 2;
        }else if("135".equals(prePhone)){
            partition = 3;
        }else {
            partition = 4;
        }

        return partition;
    }
}
