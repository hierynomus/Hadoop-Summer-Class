package com.xebia.summerclass.hadoop.weather.mapreduce.rain;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class PrecipitationPerMonthMapperTest {
    private MapDriver<LongWritable, Text, Text, LongWritable> driver;
    private List<Pair<Text, LongWritable>> output;

    @Before
    public void setUp() throws Exception {
        driver = new MapDriver<LongWritable, Text, Text, LongWritable>(new PrecipitationPerMonthMapper());
    }

    @Test
    public void shouldExtractDataFromMeasurementLine() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                       .withInputValue(new Text("  240,20100106,  186,   32,   34,   60,   17,   20,    8,   80,   17,  -28,  -66,   24,    3,   15,  -80,   24,   55,   70,  449,   29,   28,   13,   16,10036,10057,   23,10022,    4,    3,   16,   58,   14,    5,   95,   98,    4,   87,   13,    4"))
                       .run();

        assertThat(output.size(), is(1));
        assertThat(output.get(0).getFirst(), equalTo(new Text("240,201001")));
        assertThat(output.get(0).getSecond(), equalTo(new LongWritable(28)));
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
    public void shouldExtractDataWithNeglectablePrecipitationAsZero() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                       .withInputValue(new Text("  240,20100101,   28,   42,   46,   80,    3,    0,   21,  130,    4,  -16,  -63,   22,    8,   13,  -86,   24,   50,   64,  343,    0,   -1,   -1,    1,10030,10094,   24, 9985,    3,   60,    1,   80,   15,    3,   82,   95,   24,   69,   15,    3"))
                       .run();

        assertThat(output.size(), is(1));
        assertThat(output.get(0).getFirst(), equalTo(new Text("240,201001")));
        assertThat(output.get(0).getSecond(), equalTo(new LongWritable(0)));
    }

    @Test
    public void shouldNotExtractBlankPrecipitation() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                        .withInputValue(new Text("  240,20100106,  186,   32,   34,   60,   17,   20,    8,   80,   17,  -28,  -66,   24,    3,   15,  -80,   24,   55,   70,  449,   29,     ,   13,   16,10036,10057,   23,10022,    4,    3,   16,   58,   14,    5,   95,   98,    4,   87,   13,    4"))
                       .run();

        assertThat(output.size(), is(0));
    }
}
