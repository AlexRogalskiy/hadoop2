package com.c4soft.util.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import com.c4soft.util.hadoop.PostcodeUtil;

public class PostcodeUtilTest {
    private static FileSystem fs;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        fs = FileSystem.get(new Configuration());
    }

    @Test
    public void testCacheMatchers() throws IOException {
        Collection<Pattern> patterns = PostcodeUtil.cacheMatchers(fs, new Path("target/test-classes/postcodes.txt"), "FR");
        assertEquals(3, patterns.size());

        Set<String> patternStrings = new HashSet<String>(3);
        for (Pattern p : patterns) {
            patternStrings.add(p.toString());
        }
        assertTrue(patternStrings.contains("((FR)?-?)130\\d{2}"));
        assertTrue(patternStrings.contains("((FR)?-?)75\\d{3}"));
        assertTrue(patternStrings.contains("((FR)?-?)92\\d{3}"));
    }

    @Test
    public void testMatches() throws IOException {
        Collection<Pattern> frPatterns = PostcodeUtil.cacheMatchers(fs, new Path("target/test-classes/postcodes.txt"), "FR");
        assertTrue(PostcodeUtil.matches("92100", frPatterns));
        assertTrue(PostcodeUtil.matches("FR92100", frPatterns));
        assertTrue(PostcodeUtil.matches("FR-92100", frPatterns));
        assertFalse(PostcodeUtil.matches("F92100", frPatterns));
        assertFalse(PostcodeUtil.matches("04000", frPatterns));
    }
}
