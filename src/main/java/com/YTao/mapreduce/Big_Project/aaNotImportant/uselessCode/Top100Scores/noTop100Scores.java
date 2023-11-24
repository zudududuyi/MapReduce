package com.YTao.mapreduce.Big_Project.aaNotImportant.uselessCode.Top100Scores;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class noTop100Scores {

    public static class ScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");

            // 提取科目和成绩
            int id = Integer.parseInt(fields[0]);
            int score = Integer.parseInt(fields[1]);

            // 发射科目作为键，成绩作为值
            context.write(new Text(String.valueOf(score)), new IntWritable(id));
        }
    }

    public static class Top100Reducer extends Reducer<Text, IntWritable, NullWritable, Text> {

        private TreeMap<Integer, List<String>> top100Scores = new TreeMap<>(Collections.reverseOrder());

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable value : values) {
                int score = value.get();
                String id = key.toString();

                // 添加成绩和科目到TreeMap中
                if (!top100Scores.containsKey(score)) {
                    top100Scores.put(score, new ArrayList<>());
                }
                top100Scores.get(score).add(id);

                // 限制TreeMap的大小为100，只保留前100名
                if (top100Scores.size() > 100) {
                    top100Scores.pollLastEntry();
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 输出前100名成绩和科目
            for (Map.Entry<Integer, List<String>> entry : top100Scores.entrySet()) {
                int score = entry.getKey();
                List<String> ids = entry.getValue();

                for (String id : ids) {
                    context.write(NullWritable.get(), new Text(id + "\t" + score));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 100 Scores");
        job.setJarByClass(noTop100Scores.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(Top100Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(6); // 控制输出文件数目

        // 设置输入路径和输出路径
//        FileInputFormat.addInputPaths(job, args[0]); // 多个输入文件
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputA"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputC1"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}