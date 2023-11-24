package com.YTao.mapreduce.Big_Project.aaNotImportant.uselessCode.C100Before;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScoreStatistics {

    public static class ScoreMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text studentId = new Text();
        private IntWritable score = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 9) {
                String student = fields[0];
                for (int i = 1; i <= 6; i++) {
                    int subjectScore = Integer.parseInt(fields[i]);
                    studentId.set(student);
                    score.set(subjectScore);
                    context.write(studentId, score);
                }
            }
        }
    }

    public static class ScoreReducer extends Reducer<Text, IntWritable, Text, Text> {
        private TreeMap<Integer, String> topScores = new TreeMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder outputValue = new StringBuilder();
            for (IntWritable value : values) {
                int score = value.get();
                outputValue.append(score).append("\t");
            }
            context.write(key, new Text(outputValue.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Score Statistics");
        job.setJarByClass(ScoreStatistics.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputA"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputC13"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
