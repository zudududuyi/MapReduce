package com.YTao.mapreduce.Big_Project.aaNotImportant.uselessCode.Top100Scores;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top100Scoresno {

    public static class ScoreMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");

            // 提取成绩字段和学生信息
            int[] scores = { Integer.parseInt(fields[2]), Integer.parseInt(fields[3]), Integer.parseInt(fields[4]),
                    Integer.parseInt(fields[5]), Integer.parseInt(fields[6]), Integer.parseInt(fields[7]) };
            String studentInfo = fields[0] + "\t" + fields[1] + "\t" + fields[8];

            // 发射各科成绩作为键，学生信息作为值
            for (int i = 0; i < scores.length; i++) {
                context.write(new IntWritable(scores[i]), new Text(studentInfo));
            }
        }
    }

    public static class ScoreReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

        private TreeMap<Integer, List<String>> top100Scores = new TreeMap<>(Collections.reverseOrder());

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                String studentInfo = value.toString();
                int score = key.get();

                // 添加成绩和学生信息到TreeMap中
                if (!top100Scores.containsKey(score)) {
                    top100Scores.put(score, new ArrayList<>());
                }
                top100Scores.get(score).add(studentInfo);

                // 限制TreeMap的大小为100，只保留前100名
                if (top100Scores.size() > 100) {
                    top100Scores.pollLastEntry();
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 输出前100名成绩和学生信息
            for (Map.Entry<Integer, List<String>> entry : top100Scores.entrySet()) {
                int score = entry.getKey();
                List<String> students = entry.getValue();

                for (String student : students) {
                    context.write(new Text(student), new IntWritable(score));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 100 Scores");
        job.setJarByClass(Top100Scoresno.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1); // 控制输出文件数目

        // 设置输入路径和输出路径
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputBData"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputDData"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}