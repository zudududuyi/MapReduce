package com.YTao.mapreduce.Big_Project.aaNotImportant.uselessCode;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top100Scores1 {

    public static class ScoreMapper extends Mapper<Object, Text, Text, Text> {
        private Text subject = new Text();
        private Text score = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            if (fields.length == 9) {
                String studentId = fields[0];
                String languageScore = fields[1];
                String mathScore = fields[2];
                String englishScore = fields[3];
                String politicsScore = fields[4];
                String biologyScore = fields[5];
                String physicsScore = fields[6];
                String totalScore = fields[7];
                String province = fields[8];

                subject.set("Language");
                score.set(studentId + "\t" + languageScore + "\t" + province);
                context.write(subject, score);

                subject.set("Math");
                score.set(studentId + "\t" + mathScore + "\t" + province);
                context.write(subject, score);

                // Add other subjects here

                subject.set("Total");
                score.set(studentId + "\t" + totalScore + "\t" + province);
                context.write(subject, score);
            }
        }
    }

    public static class ScoreReducer extends Reducer<Text, Text, Text, Text> {
        private PriorityQueue<String> top100Scores = new PriorityQueue<>(Comparator.comparingInt(s -> Integer.parseInt(s.split("\t")[1])));

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                top100Scores.offer(value.toString());
                if (top100Scores.size() > 100) {
                    top100Scores.poll();
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (!top100Scores.isEmpty()) {
                context.write(new Text(), new Text(top100Scores.poll()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top100Scores");
        job.setJarByClass(Top100Scores1.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputA"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputC33"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}