# Lab5: avro
**Target:** Use a file format that is:
* structured (records match a schema)
* compressed (faster shuffle)
* HDFS compatible (blocs can be processed individually)
* well integrated into hadoop ecosystem
* is provided with advanced serialization framework in many languages (including Java)

**Start point:** Avro schemas are available in src/main/avro. Mapper and reducer unit tests are provided. 
Mapper and reducer are to be done. Job test is to be done too.

## Dependencies and build
1. Edit pom.xml and add a dependency on *avro-mapred*. Be careful to provide a classifier (a *hadoop.classifier* property is defined in parent pom).
2. Also activate *avro-maven-plugin* in build -> plugins section (managed from parent). This plugin will generate java classes matchin Avro schemas.
3. If src/main/avro or target/generated-sources right click those folders and select "Build Path -> Use as Source Folder"

## Avro aware job
1. Create a *BillByProductIdAvroMapper* which indexes bills by product id
* Hint for input: Avro files are red in way that the record is provided as key and value is *NullWritable*.
* This mapper should have *LongWritable* and *AvroValue<SerializableBill>* as key and value types.
* *BillByProductIdAvroMapperTest* should pass.
2. Create a *BillByLongAvroReducer* that collects lists of bills referring to a product id
* input types should be the same as mapper output
* output will contain solely records (as *AvroKey<LongIndexedSerializableBill>* with *NullWritable* values)
* *BillByLongAvroReducerTest* should pass.
3. Complete job-context TODOs

## Avro aware tests
1. Create a test class for the job
2. Use *AvroTestUtil.dumpRecords* in an *@Before* method to create input files for your tests
3. In a test, instantiate *ClassPathXmlApplicationContext* to boostrap and start the job
4. Use *AvroTestUtil.extractRecords* to assert generated file content
5. Browse through mapper and reducer tests and notice how drivers are configured using *AvroTestUtil*