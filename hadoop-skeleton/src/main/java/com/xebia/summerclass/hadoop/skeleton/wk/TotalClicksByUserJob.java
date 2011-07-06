package com.xebia.summerclass.hadoop.skeleton.wk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

import static java.lang.System.out;

public class TotalClicksByUserJob {
    public static Job createJob(Configuration configuration, Path inputPath, Path outputPath) throws IOException {
        // the default separator char is a tab and we want to produce CSV-compatible data
        configuration.set("mapred.textoutputformat.separator", ",");

        Job job = new Job(configuration);
        job.setJarByClass(TotalClicksByUserJob.class);
        job.setJobName(jobName());

        // one reducer will result in one CSV file and our output won't be big enough to cause problems.
        job.setNumReduceTasks(1);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(TotalClicksByUserMapper.class);
        job.setReducerClass(TotalByKeyReducer.class);
        job.setCombinerClass(TotalByKeyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

	public static String jobName() {
		return "total-clicks-per-user";
	}

	public static void printJobDescription() {
        out.println(jobName() + ":");
        out.println("  Performs count by clicks per user.");
        out.println();
	}
}
