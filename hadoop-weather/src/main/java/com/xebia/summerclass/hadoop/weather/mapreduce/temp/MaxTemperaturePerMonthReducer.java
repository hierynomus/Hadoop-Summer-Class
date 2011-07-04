package com.xebia.summerclass.hadoop.weather.mapreduce.temp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MaxTemperaturePerMonthReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    private static final Logger LOG = Logger.getLogger(MaxTemperaturePerMonthReducer.class);

    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        try {
            long maxValue = findMaxValue(values);

            context.write(key, new LongWritable(maxValue));
        } catch (Exception e) {
            LOG.warn("Exception caught during call to reduce(): ", e);
        }
    }

    private static long findMaxValue(Iterable<LongWritable> values) {
        long max = 0;

        for (LongWritable value : values) {
            max = Math.max(max, value.get());
        }

        return max;
    }
}
