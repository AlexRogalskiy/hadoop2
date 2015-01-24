package com.c4soft.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.c4soft.util.AvroTestUtil;
import com.c4soft.hadoop.PostcodeCategoryTurnoverTmpByCustomerIdReducer;
import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;

public class PostcodeCategoryTurnoverTmpByCustomerIdReducerTest {

    private ReduceDriver<LongWritable, AvroValue<PostcodeCategoryTurnoverTmp>, AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable> reduceDriver;

    @Before
    public void setUp() throws Exception {
        reduceDriver = ReduceDriver.newReduceDriver(new PostcodeCategoryTurnoverTmpByCustomerIdReducer());

        AvroTestUtil avroTestUtil = new AvroTestUtil(reduceDriver.getConfiguration());
        avroTestUtil.setMapOutputValueSchema(PostcodeCategoryTurnoverTmp.SCHEMA$);
        avroTestUtil.setOutputKeySchema(PostcodeCategoryTurnoverTmp.SCHEMA$);
    }

    @Test
    public void testReduceIndexedOnCustomerId() throws IOException {
        List<AvroValue<PostcodeCategoryTurnoverTmp>> input = new ArrayList<AvroValue<PostcodeCategoryTurnoverTmp>>(3);
        input.add(new AvroValue<PostcodeCategoryTurnoverTmp>(new PostcodeCategoryTurnoverTmp(1L, "FR-04000", null, null, 0.0)));
        input.add(new AvroValue<PostcodeCategoryTurnoverTmp>(new PostcodeCategoryTurnoverTmp(1L, null, 1L, null, 0.0)));
        input.add(new AvroValue<PostcodeCategoryTurnoverTmp>(new PostcodeCategoryTurnoverTmp(1L, null, 2L, "Test category", 1.5)));
        reduceDriver.setInput(new LongWritable(1L), input);

        reduceDriver.addOutput(new AvroKey<PostcodeCategoryTurnoverTmp>(new PostcodeCategoryTurnoverTmp(1L, "FR-04000", 1L, null, 0.0)), NullWritable.get());
        reduceDriver.addOutput(new AvroKey<PostcodeCategoryTurnoverTmp>(new PostcodeCategoryTurnoverTmp(1L, "FR-04000", 2L, "Test category", 1.5)),
                NullWritable.get());

        reduceDriver.runTest(false);
    }

}
