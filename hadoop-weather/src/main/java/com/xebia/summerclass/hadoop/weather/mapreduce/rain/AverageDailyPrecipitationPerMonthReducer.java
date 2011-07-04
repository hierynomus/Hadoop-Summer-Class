package com.xebia.summerclass.hadoop.weather.mapreduce.rain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class AverageDailyPrecipitationPerMonthReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    private static final Logger LOG = Logger.getLogger(AverageDailyPrecipitationPerMonthReducer.class);

    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        try {
            long average = Math.round(averageAllValues(values));

            context.write(key, new LongWritable(average));
        } catch (Exception e) {
            LOG.warn("Exception caught during call to reduce(): ", e);
        }
    }

    private static double averageAllValues(Iterable<LongWritable> values) {
        double sum = 0;
        long count = 0;

        for (LongWritable value : values) {
            sum += value.get();
            count++;
        }

        return sum / count;
    }
}
