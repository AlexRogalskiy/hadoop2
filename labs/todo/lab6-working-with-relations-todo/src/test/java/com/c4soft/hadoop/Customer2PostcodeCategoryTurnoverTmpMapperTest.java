package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.c4soft.util.AvroTestUtil;
import com.c4soft.hadoop.Customer2PostcodeCategoryTurnoverTmpMapper;
import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;
import com.c4soft.hadoop.avro.SerializableCustomer;

public class Customer2PostcodeCategoryTurnoverTmpMapperTest {

    private MapDriver<AvroKey<SerializableCustomer>, NullWritable, AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable> mapDriver;

    @Before
    public void setUp() throws Exception {
        Customer2PostcodeCategoryTurnoverTmpMapper mapper = new Customer2PostcodeCategoryTurnoverTmpMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        AvroTestUtil avroTestUtil = new AvroTestUtil(mapDriver.getConfiguration());
        avroTestUtil.setInputKeySchema(SerializableCustomer.SCHEMA$);
        avroTestUtil.setMapOutputKeySchema(PostcodeCategoryTurnoverTmp.SCHEMA$);
    }

    @Test
    public void testMap() throws IOException {
        SerializableCustomer customer = new SerializableCustomer(1L, "Jerome", "Wacongne", "FR-04000");
        mapDriver.setInput(new AvroKey<SerializableCustomer>(customer), NullWritable.get());

        PostcodeCategoryTurnoverTmp tmp = new PostcodeCategoryTurnoverTmp(1L, "FR-04000", null, null, 0.0);
        mapDriver.addOutput(new AvroKey<PostcodeCategoryTurnoverTmp>(tmp), NullWritable.get());

        mapDriver.runTest();
    }

}
