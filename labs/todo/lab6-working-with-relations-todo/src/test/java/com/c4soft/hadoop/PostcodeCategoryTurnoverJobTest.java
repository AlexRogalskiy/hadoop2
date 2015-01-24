package com.c4soft.hadoop;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.c4soft.util.AvroTestUtil;
import com.c4soft.hadoop.avro.PostcodeCategoryTurnover;
import com.c4soft.hadoop.avro.SerializableBill;
import com.c4soft.hadoop.avro.SerializableCustomer;
import com.c4soft.hadoop.avro.SerializableProduct;

public class PostcodeCategoryTurnoverJobTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        AvroTestUtil.dumpRecords(new File("target/test-classes/input/bills.avro"), SerializableBill.SCHEMA$, TestData.testBills());
        AvroTestUtil.dumpRecords(new File("target/test-classes/input/customers.avro"), SerializableCustomer.SCHEMA$, TestData.testCustomers());
        AvroTestUtil.dumpRecords(new File("target/test-classes/input/products.avro"), SerializableProduct.SCHEMA$, TestData.testProducts());
    }

    @Test
    public void test() throws IOException, InstantiationException, IllegalAccessException {
        new ClassPathXmlApplicationContext("job-context.xml").close();
        File outFile = new File("target/test-classes/output/Postcode-Category_Turnover/part-r-00000.avro");
        List<PostcodeCategoryTurnover> data = AvroTestUtil.extractRecords(outFile, PostcodeCategoryTurnover.SCHEMA$);

        assertEquals(3, data.size());

        PostcodeCategoryTurnover actual = data.get(0);
        assertEquals("FR-04000", actual.getPostcode().toString());
        assertEquals("Cooking", actual.getCategoryLabel().toString());
        assertEquals(Double.valueOf(599.0), actual.getAmount());

        actual = data.get(1);
        assertEquals("FR-04000", actual.getPostcode().toString());
        assertEquals("High tech", actual.getCategoryLabel().toString());
        assertEquals(Double.valueOf(398.0), actual.getAmount());

        actual = data.get(2);
        assertEquals("FR-92100", actual.getPostcode().toString());
        assertEquals("High tech", actual.getCategoryLabel().toString());
        assertEquals(Double.valueOf(1395.0), actual.getAmount());
    }

}
