package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;
import com.c4soft.hadoop.avro.SerializableBill;

public class Bill2PostcodeCategoryTurnoverTmpMapper extends Mapper<AvroKey<SerializableBill>, NullWritable, AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable> {

    @Override
    protected void map(AvroKey<SerializableBill> key, NullWritable value, Context context) throws IOException, InterruptedException {
        SerializableBill bill = key.datum();
        PostcodeCategoryTurnoverTmp tmp = new PostcodeCategoryTurnoverTmp(bill.getCustomerId(), null, null, null, 0.0);

        for (Long productId : bill.getProductIds()) {
            tmp.setProductId(productId);
            context.write(new AvroKey<PostcodeCategoryTurnoverTmp>(tmp), NullWritable.get());
        }
    }

}
