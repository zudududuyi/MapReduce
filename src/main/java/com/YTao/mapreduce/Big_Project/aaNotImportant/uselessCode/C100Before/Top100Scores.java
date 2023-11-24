package com.YTao.mapreduce.Big_Project.aaNotImportant.uselessCode.C100Before;

// 导入所需的Hadoop库
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Top100Scores {

    // Mapper类
    public static class ScoreMapper extends Mapper<Object, Text, Text, Text> {

        private TreeMap<Integer, String> topScores;

        @Override
        protected void setup(Context context) {
            topScores = new TreeMap<>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(" ");

            String studentId = fields[0]; // 考号

            // 遍历各科成绩
            for (int i = 1; i < fields.length - 1; i++) {
                int score = Integer.parseInt(fields[i]); // 各科成绩

                // 将成绩和考号作为键值对添加到TreeMap中
                topScores.put(score, studentId);

                // 限制TreeMap的大小为100，保留前100名
                if (topScores.size() > 100) {
                    topScores.remove(topScores.firstKey());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 发射各科前100名成绩
            for (Map.Entry<Integer, String> entry : topScores.entrySet()) {
                int score = entry.getKey();
                String studentId = entry.getValue();
                context.write(new Text("Subject"), new Text(studentId + " " + score));
            }
        }
    }

    // Reducer类
    public static class Top100Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 输出各科前100名成绩
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 创建Job实例
        Job job = Job.getInstance();
        job.setJarByClass(Top100Scores.class);
        job.setJobName("Top100Scores");

        // 设置Mapper和Reducer类
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(Top100Reducer.class);

        // 设置Mapper的输出键值类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reducer的输出键值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(6); // 控制输出文件数目

        // 设置输入和输出路径
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputA"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputC11"));

        // 提交Job并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}