package com.c4soft.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.c4soft.hadoop.avro.LongIndexedSerializableBill;
import com.c4soft.hadoop.avro.SerializableBill;

public class BillByLongAvroReducer extends Reducer<LongWritable, AvroValue<SerializableBill>, AvroKey<LongIndexedSerializableBill>, NullWritable> {

    @Override
    protected void reduce(LongWritable idx, Iterable<AvroValue<SerializableBill>> bills, Context context) throws IOException, InterruptedException {
        LongIndexedSerializableBill.Builder builder = LongIndexedSerializableBill.newBuilder();
        builder.setIdx(idx.get());

        // A product is likely to be several times in the same bill (i.e.
        // quantity > 1)
        // Using a Map to keep a single instance in such cases
        Map<Long, SerializableBill> allBills = new HashMap<Long, SerializableBill>();
        for (AvroValue<SerializableBill> value : bills) {
            SerializableBill bill = value.datum();
            if (!allBills.containsKey(bill.getId())) {
                allBills.put(bill.getId(), SerializableBill.newBuilder(bill).build());
            }
        }
        builder.setBills(new ArrayList<SerializableBill>(allBills.values()));

        context.write(new AvroKey<LongIndexedSerializableBill>(builder.build()), NullWritable.get());
    }

}
