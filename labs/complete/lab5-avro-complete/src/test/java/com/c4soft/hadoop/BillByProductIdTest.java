package com.c4soft.hadoop;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.c4soft.util.AvroTestUtil;
import com.c4soft.hadoop.avro.LongIndexedSerializableBill;
import com.c4soft.hadoop.avro.SerializableBill;

public class BillByProductIdTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        File billsFile = new File("target/test-classes/input/bills.avro");
        Calendar parisCal = new GregorianCalendar(TimeZone.getTimeZone("Europe/Paris"));
        List<SerializableBill> bills = new ArrayList<SerializableBill>(2);

        // First bill has two lines: one product 1 and an other for product 2
        List<Long> order1Items = new ArrayList<Long>(2);
        order1Items.add(1L);
        order1Items.add(3L);
        parisCal.set(2012, 9, 31, 9, 15);
        bills.add(new SerializableBill(1L, 1L, parisCal.getTimeInMillis(), order1Items));

        // second bill has four lines: three for product 2 and one for product 3
        List<Long> order2Items = new ArrayList<Long>(4);
        order2Items.add(2L);
        order2Items.add(3L);
        order2Items.add(2L);
        order2Items.add(2L);
        parisCal.set(2013, 2, 25, 14, 30);
        bills.add(new SerializableBill(2L, 2L, parisCal.getTimeInMillis(), order2Items));

        // Crate an avro file containing the bills defined above
        // This file will be used as test input
        AvroTestUtil.dumpRecords(billsFile, SerializableBill.SCHEMA$, bills);
    }

    @Test
    public void test() throws IOException, InstantiationException, IllegalAccessException {
        new ClassPathXmlApplicationContext("job-context.xml").close();
        File outFile = new File("target/test-classes/output/bills/byproductid/part-r-00000.avro");
        List<LongIndexedSerializableBill> data = AvroTestUtil.extractRecords(outFile, LongIndexedSerializableBill.SCHEMA$);
        // three different products spread in two bills
        assertEquals(3, data.size());
        assertEquals(Long.valueOf(1L), data.get(0).getIdx());
        assertEquals(Long.valueOf(2L), data.get(1).getIdx());
        assertEquals(Long.valueOf(3L), data.get(2).getIdx());

        // product 1 only in bill 1
        assertEquals(1, data.get(0).getBills().size());
        assertEquals(Long.valueOf(1L), data.get(0).getBills().get(0).getId());

        // product 2 only in bill 2
        assertEquals(1, data.get(1).getBills().size());
        assertEquals(Long.valueOf(2L), data.get(1).getBills().get(0).getId());

        // product 3 both in bills 1 & 2
        assertEquals(2, data.get(2).getBills().size());
    }

}
