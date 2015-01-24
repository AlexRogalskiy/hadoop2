package com.c4soft.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class PostcodeReducerTest {

    private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

    @Before
    public void before() {
        PostcodeReducer reducer = new PostcodeReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReduceTextIterableOfLongWritableContext() throws IOException {
        List<LongWritable> values = new ArrayList<LongWritable>(2);
        values.add(new LongWritable(1L));
        values.add(new LongWritable(2L));
        reduceDriver.withInput(new Text("FR-04000"), values);
        reduceDriver.withOutput(new Text("FR-04000"), new LongWritable(3L));
        reduceDriver.runTest();
    }

}
