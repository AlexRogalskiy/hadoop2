package com.c4soft.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;

public class PostcodeCategoryTurnoverTmpByProductIdReducer extends
        Reducer<LongWritable, AvroValue<PostcodeCategoryTurnoverTmp>, AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable> {

    @Override
    protected void reduce(LongWritable idx, Iterable<AvroValue<PostcodeCategoryTurnoverTmp>> fragments, Context context) throws IOException,
            InterruptedException {
        List<CustomerData> customersData = new ArrayList<CustomerData>();
        CharSequence productCategory = null;
        double amount = 0.0;

        for (AvroValue<PostcodeCategoryTurnoverTmp> value : fragments) {
            PostcodeCategoryTurnoverTmp fragment = value.datum();
            if (fragment.getClientId() != null) {
                customersData.add(new CustomerData(fragment));
            } else {
                productCategory = fragment.getCategoryLabel();
                amount = fragment.getAmount();
            }
        }

        PostcodeCategoryTurnoverTmp.Builder merged = PostcodeCategoryTurnoverTmp.newBuilder();
        merged.setProductId(idx.get());
        merged.setCategoryLabel(productCategory);
        merged.setAmount(amount);

        for (CustomerData customerData : customersData) {
            merged.setClientId(customerData.id);
            merged.setPostcode(customerData.postcode);
            context.write(new AvroKey<PostcodeCategoryTurnoverTmp>(merged.build()), NullWritable.get());
        }
    }

    private final static class CustomerData {
        public final long id;
        public final CharSequence postcode;

        public CustomerData(PostcodeCategoryTurnoverTmp tmp) {
            this.id = tmp.getClientId();
            this.postcode = tmp.getPostcode();
        }
    }

}
