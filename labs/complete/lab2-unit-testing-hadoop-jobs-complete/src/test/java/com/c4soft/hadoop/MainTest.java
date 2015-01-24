package com.c4soft.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.c4soft.util.Junit4ClusterMapReduceTestCase;

public class MainTest extends Junit4ClusterMapReduceTestCase {
    private static String OUT_PATH = "target/test-classes/output";

    public MainTest() throws IOException {
        super(OUT_PATH);
    }

    @Test
    public void testRun() throws Exception {
        String[] args = { "target/test-classes/input", OUT_PATH };
        int status = ToolRunner.run(new Main(), args);
        assertEquals(0, status);
        List<String> lines = outFileTextContent("part-r-00000");
        assertEquals(2, lines.size());
        assertTrue(lines.contains("FR-04000\t3"));
        assertTrue(lines.contains("FR-92100\t1"));
    }

}
