package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.c4soft.util.AvroTestUtil;
import com.c4soft.hadoop.PostcodeCategoryTurnoverTmpByPostcodeCategoryMapper;
import com.c4soft.hadoop.avro.PostcodeCategoryTurnover;
import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;

public class PostcodeCategoryTurnoverTmpByPostcodeCategoryMapperTest {

    private MapDriver<AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable, Text, AvroValue<PostcodeCategoryTurnover>> mapDriver;

    @Before
    public void setUp() throws Exception {
        mapDriver = MapDriver.newMapDriver(new PostcodeCategoryTurnoverTmpByPostcodeCategoryMapper());

        AvroTestUtil avroTestUtil = new AvroTestUtil(mapDriver.getConfiguration());
        avroTestUtil.setInputKeySchema(PostcodeCategoryTurnoverTmp.SCHEMA$);
        avroTestUtil.setMapOutputValueSchema(PostcodeCategoryTurnover.SCHEMA$);
    }

    @Test
    public void test() throws IOException {
        PostcodeCategoryTurnoverTmp tmp = new PostcodeCategoryTurnoverTmp(1L, "FR-04000", 2L, "High tech", 299.0);
        mapDriver.setInput(new AvroKey<PostcodeCategoryTurnoverTmp>(tmp), NullWritable.get());

        mapDriver.addOutput(new Text("FR-04000_High tech"), new AvroValue<PostcodeCategoryTurnover>(
                new PostcodeCategoryTurnover("FR-04000", "High tech", 299.0)));

        mapDriver.runTest();
    }

}
