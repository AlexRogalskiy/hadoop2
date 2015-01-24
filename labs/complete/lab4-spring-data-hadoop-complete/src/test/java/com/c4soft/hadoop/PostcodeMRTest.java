package com.c4soft.hadoop;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class PostcodeMRTest {

    private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;

    @Before
    public void before() throws URISyntaxException {
        CsvFieldCountMapper mapper = new CsvFieldCountMapper();
        LongSumReducer<Text> combiner = new LongSumReducer<Text>();
        LongSumReducer<Text> reducer = new LongSumReducer<Text>();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer, combiner);
        Configuration conf = mapReduceDriver.getConfiguration();
        conf.setInt(CsvFieldCountMapper.CSV_FIELD_IDX, 2);
    }

    @Test
    public void testRun() throws IOException {
        mapReduceDriver.addInput(new LongWritable(1L), new Text("\"Jérôme\",\"Wacongne\",\"FR-04000\""));
        mapReduceDriver.addInput(new LongWritable(2L), new Text("\"Star\",\"Clay\",\"FR-92100\""));
        mapReduceDriver.addInput(new LongWritable(3L), new Text("\"C\",\"4\",\"FR-04000\""));
        mapReduceDriver.addInput(new LongWritable(4L), new Text("\"Toto\",\"Tutu\",\"FR-04000\""));
        mapReduceDriver.addOutput(new Text("FR-04000"), new LongWritable(3L));
        mapReduceDriver.addOutput(new Text("FR-92100"), new LongWritable(1L));
        mapReduceDriver.runTest();
    }

}
