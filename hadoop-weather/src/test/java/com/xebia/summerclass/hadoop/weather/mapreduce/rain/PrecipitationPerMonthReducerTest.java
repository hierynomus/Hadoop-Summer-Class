package com.xebia.summerclass.hadoop.weather.mapreduce.rain;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class PrecipitationPerMonthReducerTest {
    private ReduceDriver<Text, LongWritable, Text, LongWritable> driver;
    private List<Pair<Text, LongWritable>> output;

    @Before
    public void setUp() throws Exception {
        driver = new ReduceDriver<Text, LongWritable, Text, LongWritable>(new PrecipitationPerMonthReducer());
    }

    @Test
    public void shouldSumAllCounters() throws Exception {
        output = driver.withInputKey(new Text("20100101"))
                       .withInputValue(new LongWritable(1))
                       .withInputValue(new LongWritable(2))
                       .run();

        assertThat(output.size(), is(1));
        assertThat(output.get(0).getFirst(), equalTo(new Text("20100101")));
        assertThat(output.get(0).getSecond(), equalTo(new LongWritable(3)));
    }
}
