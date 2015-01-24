package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;

public class PostcodeCategoryTurnoverTmpByCustomerIdMapper extends
        Mapper<AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable, LongWritable, AvroValue<PostcodeCategoryTurnoverTmp>> {

    @Override
    protected void map(AvroKey<PostcodeCategoryTurnoverTmp> key, NullWritable value, Context context) throws IOException, InterruptedException {
        PostcodeCategoryTurnoverTmp tmp = key.datum();

        context.write(new LongWritable(tmp.getClientId()), new AvroValue<PostcodeCategoryTurnoverTmp>(tmp));
    }

}
