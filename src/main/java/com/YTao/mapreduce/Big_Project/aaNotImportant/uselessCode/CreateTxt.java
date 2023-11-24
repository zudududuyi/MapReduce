package com.YTao.mapreduce.Big_Project.aaNotImportant.uselessCode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class CreateTxt {
    public static void main(String[] args) {
//        生成文件
//        try {
//            File file=new File("C:\\Users\\13159\\Desktop\\Grades.txt");
//            file.createNewFile();
//        }catch(IOException e){
//            e.printStackTrace();
//        }

        Random rand = new Random();
//        String []province = new String[]{"北京","天津","上海","重庆","河北",
//            "山西","辽宁","吉林","黑龙江","江苏", "浙江","安徽",
//                "福建","江西","山东","河南","湖北","湖南","广东"};
        //生成数据
        try {
            FileWriter write=new FileWriter("C:\\Users\\13159\\Desktop\\Grades\\HuBei.txt");
            BufferedWriter bw=new BufferedWriter(write);

            int z=1,sum=0,temp;
            for (int i = 0; i < 20000; i++) {
                //1.学号
                bw.write((80000+z)+"\t");
                z++;

                //2.六门课程各科科目分数和六门总分
                temp= rand.nextInt(41)+60;
                sum+=temp;
                bw.write(temp+"\t");  //1

                temp= rand.nextInt(41)+60;
                sum+=temp;
                bw.write(temp+"\t");  //2

                temp= rand.nextInt(41)+60;
                sum+=temp;
                bw.write(temp+"\t");  //3

                temp= rand.nextInt(41)+60;
                sum+=temp;
                bw.write(temp+"\t");  //4

                temp= rand.nextInt(41)+60;
                sum+=temp;
                bw.write(temp+"\t");  //5

                temp= rand.nextInt(41)+60;
                sum+=temp;
                bw.write(temp+"\t");  //6

                bw.write(sum+"\t"); //sum

                //3.省份
//                temp = rand.nextInt(province.length)+0;
//                bw.write(province[temp]+"\n");
                bw.write("HuBei"+"\n");

                sum=0;  //总分归零
            }
            bw.close();
            write.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

}


