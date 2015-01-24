package com.c4soft.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class CsvFieldCountMapperTest {

    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;

    @Before
    public void before() {
        CsvFieldCountMapper mapper = new CsvFieldCountMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        Configuration conf = mapDriver.getConfiguration();
        conf.setInt(CsvFieldCountMapper.CSV_FIELD_IDX, 2);
        conf.set(CsvFieldCountMapper.FILTER_CACHE_FILE_NAME, "fr_urban_postcodes.txt");
        mapDriver.addCacheFile(new File("target/test-classes/referential/fr_urban_postcodes.txt").toURI());
    }

    @Test
    public void testMapLongWritableTextContext() throws IOException {
        mapDriver.withInput(new LongWritable(51L), new Text("\"Jérôme\",\"Wacongne\",\"FR-04000\""));
        mapDriver.withOutput(new Text("rural"), new LongWritable(1L));
        mapDriver.runTest();
    }

}
