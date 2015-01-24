package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;
import com.c4soft.hadoop.avro.SerializableCustomer;

public class Customer2PostcodeCategoryTurnoverTmpMapper extends
        Mapper<AvroKey<SerializableCustomer>, NullWritable, AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable> {

    @Override
    protected void map(AvroKey<SerializableCustomer> key, NullWritable value, Context context) throws IOException, InterruptedException {
        SerializableCustomer customer = key.datum();
        PostcodeCategoryTurnoverTmp tmp = new PostcodeCategoryTurnoverTmp(customer.getId(), customer.getPostcode(), null, null, 0.0);

        context.write(new AvroKey<PostcodeCategoryTurnoverTmp>(tmp), NullWritable.get());
    }

}
