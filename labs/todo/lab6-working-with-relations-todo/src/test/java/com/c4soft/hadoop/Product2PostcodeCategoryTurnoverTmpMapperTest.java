package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.c4soft.util.AvroTestUtil;
import com.c4soft.hadoop.Product2PostcodeCategoryTurnoverTmpMapper;
import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;
import com.c4soft.hadoop.avro.SerializableProduct;

public class Product2PostcodeCategoryTurnoverTmpMapperTest {

    private MapDriver<AvroKey<SerializableProduct>, NullWritable, AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable> mapDriver;

    @Before
    public void setUp() throws Exception {
        Product2PostcodeCategoryTurnoverTmpMapper mapper = new Product2PostcodeCategoryTurnoverTmpMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        AvroTestUtil avroTestUtil = new AvroTestUtil(mapDriver.getConfiguration());
        avroTestUtil.setInputKeySchema(SerializableProduct.SCHEMA$);
        avroTestUtil.setMapOutputKeySchema(PostcodeCategoryTurnoverTmp.SCHEMA$);
    }

    @Test
    public void test() throws IOException {
        SerializableProduct product = TestData.testProducts().get(0);
        mapDriver.setInput(new AvroKey<SerializableProduct>(product), NullWritable.get());

        PostcodeCategoryTurnoverTmp tmp = new PostcodeCategoryTurnoverTmp(null, null, product.getId(), product.getCategory().getLabel(), product.getPrice());
        mapDriver.addOutput(new AvroKey<PostcodeCategoryTurnoverTmp>(tmp), NullWritable.get());

        mapDriver.runTest();
    }

}
