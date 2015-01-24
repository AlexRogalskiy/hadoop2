package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.c4soft.util.AvroTestUtil;
import com.c4soft.hadoop.PostcodeCategoryTurnoverTmpByCustomerIdMapper;
import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;

public class PostcodeCategoryTurnoverTmpByCustomerIdMapperTest {

    private MapDriver<AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable, LongWritable, AvroValue<PostcodeCategoryTurnoverTmp>> mapDriver;

    @Before
    public void setUp() throws Exception {
        PostcodeCategoryTurnoverTmpByCustomerIdMapper mapper = new PostcodeCategoryTurnoverTmpByCustomerIdMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        AvroTestUtil avroTestUtil = new AvroTestUtil(mapDriver.getConfiguration());
        avroTestUtil.setInputKeySchema(PostcodeCategoryTurnoverTmp.SCHEMA$);
        avroTestUtil.setMapOutputValueSchema(PostcodeCategoryTurnoverTmp.SCHEMA$);
    }

    @Test
    public void test() throws IOException {
        PostcodeCategoryTurnoverTmp tmp = new PostcodeCategoryTurnoverTmp(1L, "FR-04000", 2L, "High tech", 299.0);
        mapDriver.setInput(new AvroKey<PostcodeCategoryTurnoverTmp>(tmp), NullWritable.get());

        mapDriver.addOutput(new LongWritable(1L), new AvroValue<PostcodeCategoryTurnoverTmp>(tmp));

        mapDriver.runTest();
    }

}
