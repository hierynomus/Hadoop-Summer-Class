package com.xebia.summerclass.hadoop.skeleton.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SkeletonReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		final LongWritable outputValue = new LongWritable();
		
		long count = 0;
		for (LongWritable value : values) {
			count += value.get();
		}
		
		outputValue.set(count);
		
		context.write(key, outputValue);
	}
}
