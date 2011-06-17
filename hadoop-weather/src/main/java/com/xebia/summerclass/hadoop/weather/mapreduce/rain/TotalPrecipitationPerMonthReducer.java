package com.xebia.summerclass.hadoop.weather.mapreduce.rain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class TotalPrecipitationPerMonthReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    private static final Logger LOG = Logger.getLogger(TotalPrecipitationPerMonthReducer.class);

    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        try {
            long summedValue = sumAllValues(values);

            context.write(key, new LongWritable(summedValue));
        } catch (Exception e) {
            LOG.warn("Exception caught during call to reduce(): ", e);
        }
    }

    private static long sumAllValues(Iterable<LongWritable> values) {
        long total = 0;

        for (LongWritable value : values) {
            total += value.get();
        }

        return total;
    }
}
