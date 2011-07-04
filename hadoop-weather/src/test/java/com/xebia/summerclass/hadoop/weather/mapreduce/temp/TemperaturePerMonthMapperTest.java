package com.xebia.summerclass.hadoop.weather.mapreduce.temp;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class TemperaturePerMonthMapperTest {
    private MapDriver<LongWritable, Text, Text, LongWritable> driver;
    private List<Pair<Text, LongWritable>> output;

    @Before
    public void setUp() throws Exception {
        driver = new MapDriver<LongWritable, Text, Text, LongWritable>(new TemperaturePerMonthMapper());
    }

    @Test
    public void shouldExtractDataFromMeasurementLine() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                       .withInputValue(new Text("  240,20100101,    1,  300,   30,   30,   60,  130,     ,  101,    0,    0,    0,   28,10231,   83,    6,   83,   91,    0,    1,    0,    1,    0"))
                       .run();

        assertThat(output.size(), is(1));
        assertThat(output.get(0).getFirst(), equalTo(new Text("240,201001")));
        assertThat(output.get(0).getSecond(), equalTo(new LongWritable(130)));
    }

    @Test
    public void shouldNotExtractEmptyLine() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                       .withInputValue(new Text("")).run();

        assertThat(output.size(), is(0));
    }

    @Test
    public void shouldNotExtractCommentLine() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                       .withInputValue(new Text("# THESE DATA CAN BE USED FREELY PROVIDED THAT THE FOLLOWING SOURCE IS ACKNOWLEDGED:"))
                       .run();

        assertThat(output.size(), is(0));
    }

    @Test
    public void shouldNotExtractBlankTemperature() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                       .withInputValue(new Text("  240,20110701,    1,  300,   30,   30,   60,     ,     ,  101,    0,    0,    0,     ,10231,   83,    6,   83,   91,    0,    1,    0,    1,    0"))
                       .run();

        assertThat(output.size(), is(0));
    }
}
