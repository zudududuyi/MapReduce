package com.YTao.mapreduce.Big_Project.Csubjects.Politic;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBean,Text, Text, FlowBean> {

    private int count = 0;
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        for (Text value:values){
            if(count < 100){
                context.write(value,key);
                count++;
            }
            else{
                break;
            }
        }
    }
}

