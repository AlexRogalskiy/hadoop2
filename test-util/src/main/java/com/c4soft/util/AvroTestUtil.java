package com.c4soft.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.hadoop.io.AvroKeyComparator;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;

/**
 * A utility class for testing Avro Mappers & Reducers<br>
 * Exposes:
 * <ul>
 * <li>static methods for working with avro files (dump and load all records at
 * once)</li>
 * <li>a replacement of AvroJob configuration methods to use with MRunit Drivers
 * </li>
 * </ul>
 * MRunit ContextDriver configuration is possible to set at construction only,
 * do you'll need one instance of this class per ContextDriver.<br>
 * What this class does is storing all configured keys and values and, if
 * necessary, builds union schemas on the fly to have Avro serialization, called
 * intensively inside MRunit, work.
 * 
 * @author Ch4mp
 */
public class AvroTestUtil {

    private Configuration conf;
    private Set<Schema> keySchemas;
    private Set<Schema> valueSchemas;

    /**
     * @param configuration
     *            MRunit ContextDriver configuration
     */
    public AvroTestUtil(Configuration configuration) {
        conf = configuration;
        keySchemas = new HashSet<Schema>(3);
        valueSchemas = new HashSet<Schema>(3);
    }

    /**
     * @param file
     *            where to write to
     * @param schema
     *            Avro records structure
     * @param records
     *            all values to write in the file
     * @throws IOException
     */
    public static <T extends SpecificRecord> void dumpRecords(File file, Schema schema, List<T> records) throws IOException {
        file.getParentFile().mkdirs();
        DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(schema);
        DataFileWriter<T> dataFileWriter = new DataFileWriter<T>(datumWriter);
        dataFileWriter.create(schema, file);
        try {
            for (T record : records) {
                dataFileWriter.append(record);
            }
        } finally {
            dataFileWriter.close();
        }
    }

    /**
     * @param file
     *            where to read from
     * @param schema
     *            Avro records structure
     * @return all values from the file
     * @throws IOException
     */
    public static <T extends SpecificRecord> List<T> extractRecords(File file, Schema schema) throws IOException {
        DatumReader<T> reader = new SpecificDatumReader<>(schema);
        DataFileReader<T> fileReader = new DataFileReader<>(file, reader);
        List<T> data = new ArrayList<T>();
        try {
	        while (fileReader.hasNext()) {
	            data.add(fileReader.next());
	        }
        } finally {
        	if(fileReader != null) {
        		fileReader.close();
        	}
        }
        return data;
    }

    public void setInputKeySchema(Schema s) {
        addToAvroKeys(s);
    }

    public void setInputValueSchema(Schema s) {
        addToAvroValues(s);
    }

    public void setMapOutputKeySchema(Schema s) {
        addToAvroKeys(s);
        conf.set("mapred.mapoutput.key.class", AvroKey.class.getName());
        conf.set("mapred.output.key.comparator.class", AvroKeyComparator.class.getName());
    }

    public void setMapOutputValueSchema(Schema s) {
        addToAvroValues(s);
        conf.set("mapred.mapoutput.value.class", AvroKey.class.getName());
        conf.set("mapred.output.value.groupfn.class", AvroKeyComparator.class.getName());
    }

    public void setOutputKeySchema(Schema s) {
        addToAvroKeys(s);
    }

    public void setOutputValueSchema(Schema s) {
        addToAvroValues(s);
    }

    private Schema addToAvroKeys(Schema s) {
        Schema schema;
        keySchemas.add(s);
        if (keySchemas.size() > 1) {
            List<Schema> types = new ArrayList<Schema>(keySchemas);
            schema = Schema.createUnion(types);
        } else {
            schema = s;
        }
        conf.set("avro.schema.input.key", schema.toString());
        conf.set("avro.schema.output.key", schema.toString());
        AvroSerialization.setKeyWriterSchema(conf, schema);
        AvroSerialization.setKeyReaderSchema(conf, schema);
        AvroSerialization.addToConfiguration(conf);
        return schema;
    }

    private Schema addToAvroValues(Schema s) {
        Schema schema;
        valueSchemas.add(s);
        if (valueSchemas.size() > 1) {
            List<Schema> types = new ArrayList<Schema>(valueSchemas);
            schema = Schema.createUnion(types);
        } else {
            schema = s;
        }
        conf.set("avro.schema.input.value", schema.toString());
        conf.set("avro.schema.output.value", schema.toString());
        AvroSerialization.setValueWriterSchema(conf, schema);
        AvroSerialization.setValueReaderSchema(conf, schema);
        AvroSerialization.addToConfiguration(conf);
        return schema;
    }
}
