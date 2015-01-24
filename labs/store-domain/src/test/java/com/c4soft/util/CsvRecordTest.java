package com.c4soft.util;

import static org.junit.Assert.*;

import java.util.Collection;

import org.junit.Test;

import com.c4soft.util.CsvRecord;

public class CsvRecordTest {

    @Test(expected=NullPointerException.class)
    public void thatBuildingCsvRecordWithNullLineThrowsNullPointerException() {
        new CsvRecord((String) null);
    }

    @Test(expected=NullPointerException.class)
    public void thatBuildingCsvRecordWithNullLinesArrayThrowsNullPointerException() {
        new CsvRecord((String[]) null);
    }

    @Test(expected=NullPointerException.class)
    public void thatBuildingCsvRecordWithNullLinesCollectionThrowsNullPointerException() {
        new CsvRecord((Collection<String>) null);
    }

    @Test
    public void testCsvRecordEmptyLine() {
        CsvRecord actual = new CsvRecord("");
        assertEquals(1, actual.size());
        assertEquals(null, actual.get(0));

        assertEquals("", actual.toString());
    }

    @Test
    public void testCsvRecordSingleField() {
        CsvRecord actual = new CsvRecord("field0");
        assertEquals(1, actual.size());
        assertEquals("field0", actual.get(0));

        assertEquals("\"field0\"", actual.toString());
    }

    @Test
    public void testCsvRecordSingleQuotedField() {
        CsvRecord actual = new CsvRecord("\"field0\"");
        assertEquals(1, actual.size());
        assertEquals("field0", actual.get(0));

        assertEquals("\"field0\"", actual.toString());
    }

    @Test
    public void testCsvRecordEmptyFields() {
        CsvRecord actual = new CsvRecord(",\t, \"\"\t, \t ,");
        assertEquals(5, actual.size());
        assertEquals(null, actual.get(0));
        assertEquals("", actual.get(1));
        assertEquals("", actual.get(2));
        assertEquals("", actual.get(3));
        assertEquals(null, actual.get(4));

        assertEquals(",\"\",\"\",\"\",", actual.toString());
    }

    @Test
    public void testCsvMixed() {
        CsvRecord actual = new CsvRecord(",field1,\"field,2\",,\"\t\"");
        assertEquals(5, actual.size());
        assertEquals(null, actual.get(0));
        assertEquals("field1", actual.get(1));
        assertEquals("field,2", actual.get(2));
        assertEquals(null, actual.get(3));
        assertEquals("\t", actual.get(4));

        assertEquals(",\"field1\",\"field,2\",,\"\t\"", actual.toString());
    }

}
