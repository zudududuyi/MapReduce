package com.YTao.mapreduce.Big_Project.AWritableComparable_SortAll;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBean,Text, Text, FlowBean> {

//    private int count = 0;
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
//        if(count>=100){
//            return;
//        }
//        count++;
        //遍历 values 集合,循环写出,避免总成绩相同的情况
        for (Text value : values) {
            //调换 KV 位置,反向写出
            context.write(value,key);
        }

    }
}

