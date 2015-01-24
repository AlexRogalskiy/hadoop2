package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.c4soft.hadoop.avro.PostcodeCategoryTurnover;
import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;

public class PostcodeCategoryTurnoverTmpByPostcodeCategoryMapper extends
        Mapper<AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable, Text, AvroValue<PostcodeCategoryTurnover>> {

    @Override
    protected void map(AvroKey<PostcodeCategoryTurnoverTmp> key, NullWritable value, Context context) throws IOException, InterruptedException {
        PostcodeCategoryTurnoverTmp tmp = key.datum();

        StringBuilder outKey = new StringBuilder();
        outKey.append(tmp.getPostcode());
        outKey.append('_');
        outKey.append(tmp.getCategoryLabel());

        context.write(new Text(outKey.toString()),
                new AvroValue<PostcodeCategoryTurnover>(new PostcodeCategoryTurnover(tmp.getPostcode(), tmp.getCategoryLabel(), tmp.getAmount())));
    }

}
