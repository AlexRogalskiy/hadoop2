package com.c4soft.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.c4soft.hadoop.avro.PostcodeCategoryTurnoverTmp;

public class PostcodeCategoryTurnoverTmpByCustomerIdReducer extends
        Reducer<LongWritable, AvroValue<PostcodeCategoryTurnoverTmp>, AvroKey<PostcodeCategoryTurnoverTmp>, NullWritable> {

    @Override
    protected void reduce(LongWritable idx, Iterable<AvroValue<PostcodeCategoryTurnoverTmp>> fragments, Context context) throws IOException,
            InterruptedException {
        CharSequence postcode = null;
        Set<ProductData> products = new HashSet<ProductData>();

        for (AvroValue<PostcodeCategoryTurnoverTmp> value : fragments) {
            PostcodeCategoryTurnoverTmp fragment = value.datum();
            if (fragment.getPostcode() != null) {
                postcode = fragment.getPostcode();
            } else {
                products.add(new ProductData(fragment));
            }
        }
        if (products.size() == 0) {
            products.add(null);
        }

        PostcodeCategoryTurnoverTmp.Builder merged = PostcodeCategoryTurnoverTmp.newBuilder();
        merged.setClientId(idx.get());
        merged.setPostcode(postcode);
        merged.setAmount(0.0);

        for (ProductData product : products) {
            merged.setProductId(product.id);
            merged.setCategoryLabel(product.category);
            merged.setAmount(product.amount);
            context.write(new AvroKey<PostcodeCategoryTurnoverTmp>(merged.build()), NullWritable.get());
        }
    }

    private final static class ProductData {
        public final long id;
        public final CharSequence category;
        public final double amount;

        public ProductData(PostcodeCategoryTurnoverTmp tmp) {
            this.id = tmp.getProductId();
            this.category = tmp.getCategoryLabel();
            this.amount = tmp.getAmount();
        }
    }
}
