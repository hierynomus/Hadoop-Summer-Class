package com.xebia.summerclass.hadoop.weather.mapreduce.temp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TemperaturePerMonthTestHelper {
    // ============================================================================
    // Some quick hacks to produce test data without writing hundreds of unreadable lines
    // ============================================================================

    public static LongWritable key(long offset) {
        return new LongWritable(offset);
    }

    public static Text dataLine(long station, String datum, long temperature) {
        return new Text(
            String.format("%5d,", station) +
            datum +
            ",    0,    0,    0,    0,    0," +
            String.format("%5d,", temperature) +
            "0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0");
    }
}
