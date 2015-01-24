package com.c4soft.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.c4soft.util.CsvRecord;
import com.c4soft.util.hadoop.PostcodeUtil;

/**
 * @author Ch4mp
 * 
 */
public class CsvFieldCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable ONE = new LongWritable(1L);
    // TODO: declare a key for csv field index in job configuration (public static final String)
    // TODO: declare a key for cache file name in job configuration (public static final String)

    private static final Text URBAN = new Text("urban");
    private static final Text RURAL = new Text("rural");

    private Integer idx = null;
    private Collection<Pattern> patterns;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        // TODO: retrieve postcode patterns cache file name from job configuration
        String filterCacheFileName = null;

        // TODO: retrieve cached files from job configuration
        URI[] cachedFilesUris = null;

        for (URI cachedFile : cachedFilesUris) {
            String cachedFileStr = cachedFile.toString();
            if (cachedFileStr.contains(filterCacheFileName)) {
                FileSystem fs = FileSystem.get(cachedFile, conf);
                patterns = PostcodeUtil.cacheMatchers(fs, new Path(cachedFile), "FR");
                break;
            }
        }
        super.setup(context);
    }

    @Override
    protected void map(LongWritable kIn, Text vIn, Context context) throws IOException, InterruptedException {
        if (idx == null) {
            // TODO: retrieve csv field index from configuration
            idx = null;
        }

        String line = vIn.toString();
        CsvRecord record = new CsvRecord(line);
        String postCode = record.get(idx);

        if (PostcodeUtil.matches(postCode, patterns)) {
            context.write(URBAN, ONE);
        } else {
            context.write(RURAL, ONE);
        }
    }

}
