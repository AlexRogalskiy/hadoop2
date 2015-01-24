package com.c4soft.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.c4soft.util.AvroTestUtil;
import com.c4soft.hadoop.BillByProductIdAvroMapper;
import com.c4soft.hadoop.avro.SerializableBill;

public class BillByProductIdAvroMapperTest {

    private MapDriver<AvroKey<SerializableBill>, NullWritable, LongWritable, AvroValue<SerializableBill>> mapDriver;

    @Before
    public void before() throws IOException {
        BillByProductIdAvroMapper mapper = new BillByProductIdAvroMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        AvroTestUtil avroTestUtil = new AvroTestUtil(mapDriver.getConfiguration());
        avroTestUtil.setInputKeySchema(SerializableBill.SCHEMA$);
        avroTestUtil.setMapOutputValueSchema(SerializableBill.SCHEMA$);
    }

    @Test
    public void testMap() throws IOException {
        List<SerializableBill> bills = getTestBills();

        mapDriver.setInput(new AvroKey<SerializableBill>(bills.get(1)), NullWritable.get());
        mapDriver.addOutput(new LongWritable(2L), new AvroValue<SerializableBill>(bills.get(1)));
        mapDriver.addOutput(new LongWritable(3L), new AvroValue<SerializableBill>(bills.get(1)));

        mapDriver.runTest();
    }

    private static List<SerializableBill> getTestBills() {
        Calendar parisCal = new GregorianCalendar(TimeZone.getTimeZone("Europe/Paris"));
        List<SerializableBill> bills = new ArrayList<SerializableBill>(2);

        List<Long> order1Items = new ArrayList<Long>(2);
        order1Items.add(1L);
        order1Items.add(3L);
        parisCal.set(2012, 9, 31, 9, 15);
        bills.add(new SerializableBill(1L, 1L, parisCal.getTimeInMillis(), order1Items));

        List<Long> order2Items = new ArrayList<Long>(4);
        order2Items.add(2L);
        order2Items.add(3L);
        order2Items.add(2L);
        order2Items.add(2L);
        parisCal.set(2013, 2, 25, 14, 30);
        bills.add(new SerializableBill(2L, 2L, parisCal.getTimeInMillis(), order2Items));

        return bills;
    }

}
