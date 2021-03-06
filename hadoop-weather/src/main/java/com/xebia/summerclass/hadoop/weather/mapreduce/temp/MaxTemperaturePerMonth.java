package com.xebia.summerclass.hadoop.weather.mapreduce.temp;

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

public class MaxTemperaturePerMonth {
    public static Job createJob(Configuration configuration, Path inputPath, Path outputPath) throws IOException {
        // the default separator char is a tab and we want to produce CSV-compatible data
        configuration.set("mapred.textoutputformat.separator", ",");

        Job job = new Job(configuration);
        job.setJarByClass(MaxTemperaturePerMonth.class);
        job.setJobName(jobName());

        // one reducer will result in one CSV file and our output won't be big enough to cause problems.
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(TemperaturePerMonthMapper.class);
        job.setCombinerClass(MaxTemperaturePerMonthReducer.class);
        job.setReducerClass(MaxTemperaturePerMonthReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    public static String jobName() {
        return "max-temperature-per-month";
    }

    public static void printJobDescription() {
        out.println(jobName() + ":");
        out.println("  Processes KNMI weatherstation measurement files into a CSV file containing the station identifier,");
        out.println("  the month (yyyyMM) and the the maximum temperature (in 0.1 degree C) per month");
        out.println("  Some sample output data:");
        out.println();
        out.println("  # station, #month, #maxtemperature");
        out.println("  210,200501,173");
    }
}
