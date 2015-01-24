package com.c4soft.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.c4soft.hadoop.avro.SerializableBill;

public class BillByProductIdAvroMapper extends Mapper<AvroKey<SerializableBill>, NullWritable, LongWritable, AvroValue<SerializableBill>> {

    @Override
    protected void map(AvroKey<SerializableBill> key, NullWritable value, Context context) throws IOException, InterruptedException {
        SerializableBill bill = key.datum();
        Set<Long> uniqueProductIds = new HashSet<Long>();

        for (Long productId : bill.getProductIds()) {
            if (uniqueProductIds.add(productId)) {
                context.write(new LongWritable(productId), new AvroValue<SerializableBill>(bill));
            }
        }
    }

}
