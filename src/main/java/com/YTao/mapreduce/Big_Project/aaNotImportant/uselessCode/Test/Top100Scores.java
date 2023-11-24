package com.YTao.mapreduce.Big_Project.aaNotImportant.uselessCode.Test;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top100Scores {

    public static class ScoreMapper extends Mapper<Object, Text, Text, Text> {

        private TreeMap<Integer, String> topScoresMap;

        @Override
        protected void setup(Context context) {
            topScoresMap = new TreeMap<>();
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            if (tokens.length != 9) {
                // 忽略不完整的数据行
                return;
            }

            // 提取学号和总分
            String examNumber = tokens[0];
            int totalScore = Integer.parseInt(tokens[7]);

            // 将学号和总分添加到有序映射中
            topScoresMap.put(totalScore, examNumber);

            // 保持映射中的元素数量为100
            if (topScoresMap.size() > 100) {
                topScoresMap.remove(topScoresMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : topScoresMap.entrySet()) {
                int totalScore = entry.getKey();
                String examNumber = entry.getValue();

                // 发送学号和总分给Reducer
                context.write(new Text(examNumber), new Text(String.valueOf(totalScore)));
            }
        }
    }

    public static class ScoreReducer extends Reducer<Text, Text, Text, Text> {

        private TreeMap<Integer, String> topScoresMap;

        @Override
        protected void setup(Context context) {
            topScoresMap = new TreeMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int totalScore = Integer.parseInt(values.iterator().next().toString());

            // 将学号和总分添加到有序映射中
            topScoresMap.put(totalScore, key.toString());

            // 保持映射中的元素数量为100
            if (topScoresMap.size() > 100) {
                topScoresMap.remove(topScoresMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : topScoresMap.entrySet()) {
                int totalScore = entry.getKey();
                String examNumber = entry.getValue();

                // 发送学号和总分给输出
                context.write(new Text(examNumber), new Text(String.valueOf(totalScore)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 100 Scores");
        job.setJarByClass(Top100Scores.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 输入和输出路径从命令行参数获取
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputA"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputCASD"));

        // 提交作业并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}