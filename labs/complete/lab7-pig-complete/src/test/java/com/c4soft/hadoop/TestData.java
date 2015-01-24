package com.c4soft.hadoop;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import com.c4soft.hadoop.avro.SerializableBill;
import com.c4soft.hadoop.avro.SerializableCategory;
import com.c4soft.hadoop.avro.SerializableCustomer;
import com.c4soft.hadoop.avro.SerializableProduct;

public class TestData {

    public static List<SerializableBill> testBills() {
        List<SerializableBill> testBills = new ArrayList<SerializableBill>(3);
        Calendar parisCal = new GregorianCalendar(TimeZone.getTimeZone("Europe/Paris"));
        List<SerializableCustomer> testCustomers = testCustomers();
        List<SerializableProduct> testProducts = testProducts();

        parisCal.set(2012, 10, 31, 10, 12);
        List<Long> bill1Products = new ArrayList<Long>(2);
        bill1Products.add(testProducts.get(0).getId());
        bill1Products.add(testProducts.get(2).getId());
        testBills.add(new SerializableBill(1L, testCustomers.get(0).getId(), parisCal.getTimeInMillis(), bill1Products));

        parisCal.set(2012, 12, 01, 14, 15);
        List<Long> bill2Products = new ArrayList<Long>(5);
        bill2Products.add(testProducts.get(1).getId());
        bill2Products.add(testProducts.get(1).getId());
        bill2Products.add(testProducts.get(1).getId());
        bill2Products.add(testProducts.get(0).getId());
        bill2Products.add(testProducts.get(1).getId());
        testBills.add(new SerializableBill(2L, testCustomers.get(1).getId(), parisCal.getTimeInMillis(), bill2Products));

        parisCal.set(2013, 02, 15, 19, 51);
        List<Long> bill3Products = new ArrayList<Long>(1);
        bill3Products.add(testProducts.get(0).getId());
        testBills.add(new SerializableBill(3L, testCustomers.get(0).getId(), parisCal.getTimeInMillis(), bill3Products));

        return testBills;
    }

    public static List<SerializableCustomer> testCustomers() {
        List<SerializableCustomer> testCustomers = new ArrayList<SerializableCustomer>(2);

        testCustomers.add(new SerializableCustomer(1L, "Jerome", "Wacongne", "FR-04000"));
        testCustomers.add(new SerializableCustomer(2L, "Foo", "Bar", "FR-92100"));

        return testCustomers;
    }

    public static List<SerializableProduct> testProducts() {
        List<SerializableProduct> testProducts = new ArrayList<SerializableProduct>(3);
        SerializableCategory highTechCategory = new SerializableCategory(1L, "High tech");
        SerializableCategory cookingCategory = new SerializableCategory(1L, "Cooking");

        testProducts.add(new SerializableProduct(1L, "Nexus 7", 199.0, highTechCategory));
        testProducts.add(new SerializableProduct(2L, "Nexus 4", 299.0, highTechCategory));
        testProducts.add(new SerializableProduct(3L, "Kitchenaid Artisan", 599.0, cookingCategory));

        return testProducts;
    }

}
