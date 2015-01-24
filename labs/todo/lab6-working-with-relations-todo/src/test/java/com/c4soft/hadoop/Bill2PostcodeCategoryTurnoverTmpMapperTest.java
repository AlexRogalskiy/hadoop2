package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.c4soft.util.AvroTestUtil;
import com.c4soft.hadoop.Bill2PostcodeCategoryTurnoverTmpMapper;
import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;
import com.c4soft.hadoop.avro.SerializableBill;

public class Bill2PostcodeCategoryTurnoverTmpMapperTest {

    private MapDriver<AvroKey<SerializableBill>, NullWritable, AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable> mapDriver;

    @Before
    public void setUp() throws Exception {
        Bill2PostcodeCategoryTurnoverTmpMapper mapper = new Bill2PostcodeCategoryTurnoverTmpMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        AvroTestUtil avroTestUtil = new AvroTestUtil(mapDriver.getConfiguration());
        avroTestUtil.setInputKeySchema(SerializableBill.SCHEMA$);
        avroTestUtil.setMapOutputKeySchema(PostcodeCategoryTurnoverTmp.SCHEMA$);
    }

    @Test
    public void test() throws IOException {
        SerializableBill bill = TestData.testBills().get(1);
        mapDriver.setInput(new AvroKey<SerializableBill>(bill), NullWritable.get());

        for (Long productId : bill.getProductIds()) {
            PostcodeCategoryTurnoverTmp tmp = new PostcodeCategoryTurnoverTmp(bill.getCustomerId(), null, productId, null, 0.0);
            mapDriver.addOutput(new AvroKey<PostcodeCategoryTurnoverTmp>(tmp), NullWritable.get());
        }

        mapDriver.runTest();
    }

}
