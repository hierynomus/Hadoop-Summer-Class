package com.xebia.summerclass.hadoop.skeleton.mapreduce;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


public class SkeletonMapReduceTest {
    private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> driver;
    private List<Pair<Text, LongWritable>> output;

    @Before
    public void setUp() throws Exception {
        driver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>()
            .withMapper(new SkeletonMapper())
            .withReducer(new SkeletonReducer());
    }

    @Test
    public void shouldProduceCorrectTotalsForDataFromOneMonth() throws Exception {
        output = driver.withInput(new LongWritable(0), new Text("apple apple apple"))
                       .withInput(new LongWritable(1), new Text("apple orange orange"))
                       .withInput(new LongWritable(2), new Text("orange"))
                       .run();

        assertThat(output.size(), is(2));

        assertThat(output.get(0).getFirst(), equalTo(new Text("apple")));
        assertThat(output.get(0).getSecond(), equalTo(new LongWritable(4)));

        assertThat(output.get(1).getFirst(), equalTo(new Text("orange")));
        assertThat(output.get(1).getSecond(), equalTo(new LongWritable(3)));
    }
}
