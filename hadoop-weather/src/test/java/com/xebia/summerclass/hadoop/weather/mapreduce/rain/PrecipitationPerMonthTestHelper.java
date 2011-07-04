package com.xebia.summerclass.hadoop.weather.mapreduce.rain;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class PrecipitationPerMonthTestHelper {
    // ============================================================================
    // Some quick hacks to produce test data without writing hundreds of unreadable lines
    // ============================================================================

    public static LongWritable key(long offset) {
        return new LongWritable(offset);
    }

    public static Text dataLine(long station, String datum, long precipitation) {
        return new Text(
            String.format("%5d,", station) +
            datum +
            ",    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0," +
            String.format("%5d,", precipitation) +
            "    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0");
    }
}
