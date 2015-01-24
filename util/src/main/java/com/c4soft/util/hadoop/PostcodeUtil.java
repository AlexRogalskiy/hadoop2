package com.c4soft.util.hadoop;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PostcodeUtil {

    private static Map<String, Map<String, Collection<Pattern>>> cache = new HashMap<String, Map<String, Collection<Pattern>>>();

    public static Collection<Pattern> cacheMatchers(FileSystem fs, Path cacheFilePath, String postcodePrefix) throws IOException {
        String uriCache = cacheFilePath.toUri().toString();
        Map<String, Collection<Pattern>> pathCache = cache.get(uriCache);
        if (pathCache == null) {
            pathCache = new HashMap<String, Collection<Pattern>>();
            cache.put(uriCache, pathCache);
        }

        Collection<Pattern> patterns = pathCache.get(postcodePrefix);
        if (patterns == null) {
            patterns = new HashSet<Pattern>();

            StringBuilder sb = new StringBuilder("((");
            sb.append(postcodePrefix);
            sb.append(")?-?)");
            String regexPrefix = sb.toString();

            for (String postcodeRegex : (List<String>) IOUtils.readLines(fs.open(cacheFilePath))) {
                patterns.add(Pattern.compile(regexPrefix + postcodeRegex));
            }

            pathCache.put(postcodePrefix, patterns);
        }

        return patterns;
    }

    public static boolean matches(String postcode, Collection<Pattern> patterns) {
        for (Pattern p : patterns) {
            if (p.matcher(postcode).matches()) {
                return true;
            }
        }
        return false;
    }
}
