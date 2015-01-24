package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.c4soft.util.CsvRecord;

/**
 * @author Ch4mp
 * 
 */
public class PostcodeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable ONE = new LongWritable(1L);

    @Override
    protected void map(LongWritable kIn, Text vIn, Context context) throws IOException, InterruptedException {
        String line = vIn.toString();
        CsvRecord record = new CsvRecord(line);
        String postCode = record.get(2);
        context.write(new Text(postCode), ONE);
    }

}
