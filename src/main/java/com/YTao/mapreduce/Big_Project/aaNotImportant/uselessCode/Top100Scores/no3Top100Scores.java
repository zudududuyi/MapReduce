package com.YTao.mapreduce.Big_Project.aaNotImportant.uselessCode.Top100Scores;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class no3Top100Scores {

    public static class ScoreMapper extends Mapper<LongWritable, Text, NullWritable, IntPairWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");

            // 提取学号和成绩
            int studentId = Integer.parseInt(fields[0]);
            int score = Integer.parseInt(fields[6]);

            // 发射学号和成绩
            context.write(NullWritable.get(), new IntPairWritable(studentId, score));
        }
    }

    public static class Top100Reducer extends Reducer<NullWritable, IntPairWritable, NullWritable, IntPairWritable> {

        private TreeMap<Integer, List<Integer>> top100Scores = new TreeMap<>(Collections.reverseOrder());

        public void reduce(NullWritable key, Iterable<IntPairWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntPairWritable value : values) {
                int studentId = value.getFirst();
                int score = value.getSecond();

                // 添加成绩到TreeMap中
                if (!top100Scores.containsKey(score)) {
                    top100Scores.put(score, new ArrayList<>());
                }
                top100Scores.get(score).add(studentId);

                // 限制TreeMap的大小为100，只保留前100名
                if (top100Scores.size() > 100) {
                    top100Scores.pollLastEntry();
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 输出前100名学号和成绩
            for (Map.Entry<Integer, List<Integer>> entry : top100Scores.entrySet()) {
                int score = entry.getKey();
                List<Integer> studentIds = entry.getValue();

                for (int studentId : studentIds) {
                    context.write(NullWritable.get(), new IntPairWritable(studentId, score));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 100 Scores");
        job.setJarByClass(no3Top100Scores.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(Top100Reducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(IntPairWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntPairWritable.class);

        // 设置输入路径和输出路径
//        FileInputFormat.setInputPaths(job, args[0]); // 多个输入文件
//        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputA"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputC3"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}