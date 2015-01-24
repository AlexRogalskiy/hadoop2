package com.c4soft.util;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for Hadoop Map / Reduce Job tests on Windows<br>
 * It:
 * <ul>
 * <li>adapts from JUnit3 to JUnit4</li>
 * <li>lunches a mini MR cluster</li>
 * <li>used to work around directories permission modification bug (HADOOP_7682), but doesn't any-more</li>
 * <li>deletes output directory before each test</li>
 * </ul>
 * 
 * @author Ch4mp
 */
public class Junit4ClusterMapReduceTestCase extends ClusterMapReduceTestCase  {
    private FileSystem fs;
    private Path out;

    /**
     * @param outPath
     *            path to delete before each hadoop run
     * @throws IOException
     *             if outhPath is malformed
     */
    public Junit4ClusterMapReduceTestCase(String outPath) throws IOException {
        this(outPath, new Configuration());
    }

    /**
     * @param outPath
     *            path to delete before each hadoop run
     * @param conf
     *            specific config option to get FileSystem
     * @throws IOException
     *             if outhPath is malformed
     */
    public Junit4ClusterMapReduceTestCase(String outPath, Configuration conf) throws IOException {
        out = new Path(outPath);
        fs = FileSystem.get(conf);
    }

    @Before
    protected void setUp() throws Exception {
        if (System.getProperty("hadoop.log.dir") == null) {
            System.setProperty("hadoop.log.dir", ".");
        }
        if (System.getProperty("hadoop.log.file") == null) {
            System.setProperty("hadoop.log.file", "hadoop.log");
        }
        if (System.getProperty("hadoop.root.logger") == null) {
            System.setProperty("hadoop.root.logger", "DEBUG,console");
        }

        fs.delete(out, true);

        super.setUp();
    }

    @After
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * @param fileName
     *            relative path from output dir
     * @return one String per line
     * @throws IOException
     */
    public List<String> outFileTextContent(String fileName) throws IOException {
        Path p = new Path(out, fileName);
        List<String> lines = IOUtils.readLines(fs.open(p));
        return lines;
    }

}
