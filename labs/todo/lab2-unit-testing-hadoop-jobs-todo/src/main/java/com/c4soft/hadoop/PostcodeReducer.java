package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PostcodeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text kIn, Iterable<LongWritable> vInIterator, Context context) throws IOException, InterruptedException {
        long cnt = 0L;
        for (LongWritable subCnt : vInIterator) {
            cnt += subCnt.get();
        }
        context.write(kIn, new LongWritable(cnt));
    }

}
