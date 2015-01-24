package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.c4soft.hadoop.PostcodeMapper;

public class PostcodeMapperTest {

    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;

    @Before
    public void before() {
        PostcodeMapper mapper = new PostcodeMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapLongWritableTextContext() throws IOException {
        mapDriver.withInput(new LongWritable(51L), new Text("\"Jérôme\",\"Wacongne\",\"FR-04000\""));
        mapDriver.withOutput(new Text("FR-04000"), new LongWritable(1L));
        mapDriver.runTest();
    }

}
