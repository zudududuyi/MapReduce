package com.YTao.mapreduce.Big_Project.aaNotImportant.uselessCode;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //1 获取一行数据,转成字符串
        String line = value.toString();
        //2 切割数据
        String[] split = line.split("\t");
        //3 抓取我们需要的数据
        String id = split[0];
        String chinese = split[1];
        String math = split[2];
        String english = split[3];
        String politic = split[4];
        String biology = split[5];
        String physics = split[6];
        String sum = split[7];
        String province = split[8];

        //4 封装 outK outV
        outK.set(id);
        outV.setChinese(Integer.parseInt(chinese));
        outV.setMath(Integer.parseInt(math));
        outV.setEnglish(Integer.parseInt(english));
        outV.setPolitic(Integer.parseInt(politic));
        outV.setBiology(Integer.parseInt(biology));
        outV.setPhysics(Integer.parseInt(physics));
        outV.setSum(Integer.parseInt(sum));
        outV.setProvince(province);

        //5 写出 outK outV
        context.write(outK, outV);
    }
}
