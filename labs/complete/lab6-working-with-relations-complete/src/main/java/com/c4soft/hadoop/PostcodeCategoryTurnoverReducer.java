package com.c4soft.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.c4soft.hadoop.avro.PostcodeCategoryTurnover;

public class PostcodeCategoryTurnoverReducer extends Reducer<Text, AvroValue<PostcodeCategoryTurnover>, AvroKey<PostcodeCategoryTurnover>, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<AvroValue<PostcodeCategoryTurnover>> values, Context context) throws IOException, InterruptedException {
        PostcodeCategoryTurnover aggregated = new PostcodeCategoryTurnover();
        Iterator<AvroValue<PostcodeCategoryTurnover>> i = values.iterator();
        PostcodeCategoryTurnover value = i.next().datum();
        aggregated.setPostcode(value.getPostcode());
        aggregated.setCategoryLabel(value.getCategoryLabel());
        double totalAmount = value.getAmount();
        while (i.hasNext()) {
            totalAmount += i.next().datum().getAmount();
        }
        aggregated.setAmount(totalAmount);
        context.write(new AvroKey<PostcodeCategoryTurnover>(aggregated), NullWritable.get());
    }

}
