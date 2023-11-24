package com.YTao.mapreduce.Big_Project.DaverageScores;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageScores {

    public static class ScoreMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text subject = new Text();
        private DoubleWritable score = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 9) {
                double chineseScore = Double.parseDouble(fields[1]);
                double mathScore = Double.parseDouble(fields[2]);
                double englishScore = Double.parseDouble(fields[3]);
                double politicsScore = Double.parseDouble(fields[4]);
                double biologyScore = Double.parseDouble(fields[5]);
                double physicsScore = Double.parseDouble(fields[6]);

                subject.set("Chinese");
                score.set(chineseScore);
                context.write(subject, score);

                subject.set("Math");
                score.set(mathScore);
                context.write(subject, score);

                subject.set("English");
                score.set(englishScore);
                context.write(subject, score);

                subject.set("Politics");
                score.set(politicsScore);
                context.write(subject, score);

                subject.set("Biology");
                score.set(biologyScore);
                context.write(subject, score);

                subject.set("Physics");
                score.set(physicsScore);
                context.write(subject, score);
            }
        }
    }

    public static class ScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable average = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }
            double averageScore = sum / count;
            average.set(averageScore);
            context.write(key, average);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AverageScores");
        job.setJarByClass(AverageScores.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputA"));
//        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\13159\\Desktop\\OutPutGrades\\outputD"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
