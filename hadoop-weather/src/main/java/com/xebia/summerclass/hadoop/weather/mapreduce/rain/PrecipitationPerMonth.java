package com.xebia.summerclass.hadoop.weather.mapreduce.rain;

import static java.lang.System.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PrecipitationPerMonth {
    public static Job createJob(Configuration configuration, Path inputPath, Path outputPath) throws IOException {
        Job job = new Job(configuration);
        job.setJarByClass(PrecipitationPerMonth.class);
        job.setJobName(jobName());

        // one reducer will result in one CSV file and our output won't be big enough to cause problems.
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(PrecipitationPerMonthMapper.class);
        job.setCombinerClass(PrecipitationPerMonthReducer.class);
        job.setReducerClass(PrecipitationPerMonthReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    public static String jobName() {
        return "precipitation-per-month";
    }

    public static void printJobDescription() {
        out.println(jobName() + ":");
        out.println("  TODO");
    }
}
