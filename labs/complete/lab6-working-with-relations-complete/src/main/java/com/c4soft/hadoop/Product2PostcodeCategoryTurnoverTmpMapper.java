package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;
import com.c4soft.hadoop.avro.SerializableProduct;

public class Product2PostcodeCategoryTurnoverTmpMapper extends
        Mapper<AvroKey<SerializableProduct>, NullWritable, AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable> {

    @Override
    protected void map(AvroKey<SerializableProduct> key, NullWritable value, Context context) throws IOException, InterruptedException {
        SerializableProduct product = key.datum();
        PostcodeCategoryTurnoverTmp tmp = new PostcodeCategoryTurnoverTmp(null, null, product.getId(), product.getCategory().getLabel(), product.getPrice());

        context.write(new AvroKey<PostcodeCategoryTurnoverTmp>(tmp), NullWritable.get());
    }

}
