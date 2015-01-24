package com.c4soft.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class MainTest {

	@Test
    public void testRun() throws Exception {
        new ClassPathXmlApplicationContext("job-context.xml").close();
        File outFile = new File("target/test-classes/output/part-r-00000");
        List<String> lines = IOUtils.readLines(new FileInputStream(outFile));
        assertEquals(2, lines.size());
        assertTrue(lines.contains("FR-04000\t3"));
        assertTrue(lines.contains("FR-92100\t1"));
    }

}
