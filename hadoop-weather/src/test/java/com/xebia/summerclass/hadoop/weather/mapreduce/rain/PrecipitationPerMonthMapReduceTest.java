package com.xebia.summerclass.hadoop.weather.mapreduce.rain;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class PrecipitationPerMonthMapReduceTest {
    private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> driver;
    private List<Pair<Text, LongWritable>> output;

    @Before
    public void setUp() throws Exception {
        driver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>()
            .withMapper(new PrecipitationPerMonthMapper())
            .withReducer(new PrecipitationPerMonthReducer());
    }

    @Test
    public void shouldProduceCorrectTotalsForDataFromOneMonth() throws Exception {
        output = driver.withInput(key(0), dataLine(425, "20100101", 1))
                       .run();

        assertThat(output.size(), is(1));

        assertThat(output.get(0).getFirst(), equalTo(new Text("20100101")));
        assertThat(output.get(0).getSecond(), equalTo(new LongWritable(1)));
    }

    // ============================================================================
    // Some quick hacks to produce test data without writing hundreds of unreadable lines
    // ============================================================================

    private static LongWritable key(long offset) {
        return new LongWritable(offset);
    }

    private static Text dataLine(long station, String datum, long precipitation) {
        return new Text(
            String.format("%5d,", station) +
            datum +
            ",    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0," +
            String.format("%5d,", precipitation) +
            "    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0");
    }
}
