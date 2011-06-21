package com.xebia.summerclass.hadoop.skeleton.mapreduce;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class SkeletonMapperTest {
    private MapDriver<LongWritable, Text, Text, LongWritable> driver;
    private List<Pair<Text, LongWritable>> output;

    @Before
    public void setUp() throws Exception {
        driver = new MapDriver<LongWritable, Text, Text, LongWritable>(new SkeletonMapper());
    }

    @Test
    public void shouldExtractLowercaseWords() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                       .withInputValue(new Text("In pioneer days they used oxen for heavy pulling, and when one ox couldn’t budge a log, they didn’t try to grow a larger ox. We shouldn’t be trying for bigger computers, but for more systems of computers."))
                       .run();

        assertThat(output.size(), is(38));

        assertThat(output.get(0).getFirst(), equalTo(new Text("in")));
        assertThat(output.get(0).getSecond(), equalTo(new LongWritable(1)));

        assertThat(output.get(1).getFirst(), equalTo(new Text("pioneer")));
        assertThat(output.get(1).getSecond(), equalTo(new LongWritable(1)));
    }

    @Test
    public void shouldIgnorePunctuation() throws Exception {
        output = driver.withInputKey(new LongWritable(0))
                       .withInputValue(new Text("In pioneer, days."))
                       .run();

        assertThat(output.size(), is(3));

        assertThat(output.get(0).getFirst(), equalTo(new Text("in")));
        assertThat(output.get(1).getFirst(), equalTo(new Text("pioneer")));
        assertThat(output.get(2).getFirst(), equalTo(new Text("days")));
    }
}
