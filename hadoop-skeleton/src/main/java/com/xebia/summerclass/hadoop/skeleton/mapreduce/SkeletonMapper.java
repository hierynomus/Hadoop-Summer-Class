package com.xebia.summerclass.hadoop.skeleton.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SkeletonMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		final Text outputKey = new Text();
		final LongWritable outputValue = new LongWritable(1L);
		
		String[] words = value.toString().toLowerCase().trim().split("[\\s,\\.]+");
		for (String word: words) {
			outputKey.set(word);
			context.write(outputKey, outputValue);
		}
	}
}
