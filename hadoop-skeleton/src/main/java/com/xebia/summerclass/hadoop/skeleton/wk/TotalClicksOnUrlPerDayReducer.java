package com.xebia.summerclass.hadoop.skeleton.wk;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TotalClicksOnUrlPerDayReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long i = 0;
		for (LongWritable value : values) {
			i += value.get();
		}
		context.write(key, new LongWritable(i));
	}
}
