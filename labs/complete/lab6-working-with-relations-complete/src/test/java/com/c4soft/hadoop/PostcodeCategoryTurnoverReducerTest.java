package com.c4soft.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.c4soft.util.AvroTestUtil;
import com.c4soft.hadoop.PostcodeCategoryTurnoverReducer;
import com.c4soft.hadoop.avro.PostcodeCategoryTurnover;

public class PostcodeCategoryTurnoverReducerTest {

    private ReduceDriver<Text, AvroValue<PostcodeCategoryTurnover>, AvroKey<PostcodeCategoryTurnover>, NullWritable> driver;

    @Before
    public void setUp() throws Exception {
        driver = ReduceDriver.newReduceDriver(new PostcodeCategoryTurnoverReducer());

        AvroTestUtil avroTestUtil = new AvroTestUtil(driver.getConfiguration());
        avroTestUtil.setInputValueSchema(PostcodeCategoryTurnover.SCHEMA$);
        avroTestUtil.setMapOutputKeySchema(PostcodeCategoryTurnover.SCHEMA$);
    }

    @Test
    public void test() throws IOException {
        List<AvroValue<PostcodeCategoryTurnover>> input = new ArrayList<AvroValue<PostcodeCategoryTurnover>>(3);
        input.add(new AvroValue<PostcodeCategoryTurnover>(new PostcodeCategoryTurnover("FR-04000", "High tech", 299.0)));
        input.add(new AvroValue<PostcodeCategoryTurnover>(new PostcodeCategoryTurnover("FR-04000", "High tech", 199.0)));
        input.add(new AvroValue<PostcodeCategoryTurnover>(new PostcodeCategoryTurnover("FR-04000", "High tech", 299.0)));
        driver.setInput(new Text("FR-04000_High tech"), input);

        driver.addOutput(new AvroKey<PostcodeCategoryTurnover>(new PostcodeCategoryTurnover("FR-04000", "High tech", 797.0)), NullWritable.get());

        driver.runTest();
    }

}
