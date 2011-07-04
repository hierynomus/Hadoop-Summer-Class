package com.xebia.summerclass.hadoop.weather.mapreduce.temp;

import static com.xebia.summerclass.hadoop.weather.mapreduce.temp.TemperaturePerMonthTestHelper.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class MaxTemperaturePerMonthMapReduceTest {
    private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> driver;
    private List<Pair<Text, LongWritable>> output;

    @Before
    public void setUp() throws Exception {
        driver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>()
            .withMapper(new TemperaturePerMonthMapper())
            .withReducer(new MaxTemperaturePerMonthReducer());
    }

    @Test
    public void shouldProduceCorrectMaxForDataFromOneMonth() throws Exception {
        output = driver.withInput(key(0), dataLine(240, "20100101", 1))
                       .withInput(key(1), dataLine(260, "20100101", 3))
                       .withInput(key(2), dataLine(240, "20100102", 2))
                       .withInput(key(3), dataLine(260, "20100102", 6))
                       .run();

        assertThat(output.size(), is(2));

        assertThat(output.get(0).getFirst(), equalTo(new Text("240,201001")));
        assertThat(output.get(0).getSecond(), equalTo(new LongWritable(2)));

        assertThat(output.get(1).getFirst(), equalTo(new Text("260,201001")));
        assertThat(output.get(1).getSecond(), equalTo(new LongWritable(6)));
    }

    @Test
    public void shouldProduceCorrectMaxForDataFromMultipleMonths() throws Exception {
        output = driver.withInput(key(0), dataLine(240, "20100101", 1))
                       .withInput(key(1), dataLine(260, "20100101", 3))
                       .withInput(key(2), dataLine(240, "20100102", 2))
                       .withInput(key(3), dataLine(260, "20100102", 6))
                       .withInput(key(4), dataLine(240, "20100201", 8))
                       .withInput(key(5), dataLine(260, "20100201", 7))
                       .withInput(key(6), dataLine(240, "20100202", 6))
                       .withInput(key(7), dataLine(260, "20100202", 5))
                       .run();

        assertThat(output.size(), is(4));

        assertThat(output.get(0).getFirst(), equalTo(new Text("240,201001")));
        assertThat(output.get(0).getSecond(), equalTo(new LongWritable(2)));

        assertThat(output.get(1).getFirst(), equalTo(new Text("240,201002")));
        assertThat(output.get(1).getSecond(), equalTo(new LongWritable(8)));

        assertThat(output.get(2).getFirst(), equalTo(new Text("260,201001")));
        assertThat(output.get(2).getSecond(), equalTo(new LongWritable(6)));

        assertThat(output.get(3).getFirst(), equalTo(new Text("260,201002")));
        assertThat(output.get(3).getSecond(), equalTo(new LongWritable(7)));
    }
}
