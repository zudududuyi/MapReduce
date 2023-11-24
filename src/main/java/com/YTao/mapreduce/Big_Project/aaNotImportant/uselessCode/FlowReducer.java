package com.YTao.mapreduce.Big_Project.aaNotImportant.uselessCode;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,Text, FlowBean> {
    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        int chinese=0;
        int math=0;
        int english=0;
        int politic=0;
        int biology=0;
        int physics=0;
        int sum=0;
        String province=null;

        for(FlowBean flowBean : values){
            chinese = flowBean.getChinese();
            math = flowBean.getMath();
            english = flowBean.getEnglish();
            politic = flowBean.getPolitic();
            biology = flowBean.getBiology();
            physics = flowBean.getPhysics();
            sum = flowBean.getSum();
            province = flowBean.getProvince();
        }

        outV.setChinese(chinese);
        outV.setMath(math);
        outV.setEnglish(english);
        outV.setPolitic(politic);
        outV.setBiology(biology);
        outV.setPhysics(physics);
        outV.setSum(sum);
        outV.setProvince(province);

        context.write(key,outV);
    }
}
