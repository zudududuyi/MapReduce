package com.YTao.mapreduce.Big_Project.Csubjects.Physics;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text, FlowBean,Text> {
    private FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        //1 获取一行数据
        String line = value.toString();
        //2 按照"\t",切割数据
        String[] split = line.split("\t");

        //3 封装 outK outV
        outK.setSum(Integer.parseInt(split[7]));
        outK.setChinese(Integer.parseInt(split[1]));
        outK.setMath(Integer.parseInt(split[2]));
        outK.setEnglish(Integer.parseInt(split[3]));
        outK.setPolitic(Integer.parseInt(split[4]));
        outK.setBiology(Integer.parseInt(split[5]));
        outK.setPhysics(Integer.parseInt(split[6]));
        outK.setProvince(split[8]);
        outV.set(split[0]);

        //4 写出 outK outV
        context.write(outK,outV);
    }
}
