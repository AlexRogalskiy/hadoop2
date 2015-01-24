package com.c4soft.hadoop;

import static org.junit.Assert.assertEquals;

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
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.c4soft.util.AvroTestUtil;
import com.c4soft.hadoop.BillByLongAvroReducer;
import com.c4soft.hadoop.avro.LongIndexedSerializableBill;
import com.c4soft.hadoop.avro.SerializableBill;

public class BillByLongAvroReducerTest {

    private ReduceDriver<LongWritable, AvroValue<SerializableBill>, AvroKey<LongIndexedSerializableBill>, NullWritable> reduceDriver;

    @Before
    public void before() throws IOException {
        BillByLongAvroReducer reducer = new BillByLongAvroReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        AvroTestUtil avroTestUtil = new AvroTestUtil(reduceDriver.getConfiguration());
        avroTestUtil.setMapOutputValueSchema(SerializableBill.SCHEMA$);
        avroTestUtil.setOutputKeySchema(LongIndexedSerializableBill.SCHEMA$);
    }

    @Test
    public void testReduce() throws IOException {
        LongWritable idx = new LongWritable(3L);
        List<AvroValue<SerializableBill>> wrappedBills = new ArrayList<AvroValue<SerializableBill>>(2);
        for (SerializableBill bill : getTestBills()) {
            wrappedBills.add(new AvroValue<SerializableBill>(bill));
        }

        LongIndexedSerializableBill bills = new LongIndexedSerializableBill();
        bills.setIdx(idx.get());
        bills.setBills(getTestBills());

        reduceDriver.setInput(idx, wrappedBills);
        List<Pair<AvroKey<LongIndexedSerializableBill>, NullWritable>> outputRecords = reduceDriver.run();

        assertEquals(1, outputRecords.size());
        LongIndexedSerializableBill actualBills = outputRecords.get(0).getFirst().datum();
        assertEquals(Long.valueOf(3L), actualBills.getIdx());
        assertEquals(3, actualBills.getBills().size());
        assertEquals(Long.valueOf(1L), actualBills.getBills().get(0).getId());
        assertEquals(Long.valueOf(2L), actualBills.getBills().get(1).getId());
        assertEquals(Long.valueOf(3L), actualBills.getBills().get(2).getId());
    }

    // Three bills (same items bought by same customer on October 31st 2012 and
    // March 26th 2013, different order for different customer on March 25th
    // 2013)
    private static List<SerializableBill> getTestBills() {
        Calendar parisCal = new GregorianCalendar(TimeZone.getTimeZone("Europe/Paris"));
        List<SerializableBill> bills = new ArrayList<SerializableBill>(3);

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

        parisCal.set(2013, 2, 26, 9, 15);
        bills.add(new SerializableBill(3L, 1L, parisCal.getTimeInMillis(), order1Items));

        return bills;
    }

}
