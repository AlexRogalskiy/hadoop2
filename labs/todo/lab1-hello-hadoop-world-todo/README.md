# Lab1: Variation on the « CountWord » theme
**Target:** Create Hadoop Map / Reduce job components and run it on an authentic fake cluster.

**Start point:** project is created. It includes Maven build and unit tests with an input file and directory tree.

## Create a PostcodeMapper class:
* Have it implement *org.apache.hadoop.mapreduce.Mapper*.
* Define input types to LongWritable for the key (line number) and Text for the value (current line content).
* Define output types to Text for the key (postcode) and LongWritable the value (number of clients sharing this postcode).

## Implement *map()* method:
* extract the third field of each line (« lastName », « firstName », « posteCode »). You can use *com.c4soft.util.CsvRecord* to do this.
* collect (context.write()) this field as key and use 1 as value.

## Create PostcodeReducer class:
* Have it implement *org.apache.hadoop.mapreduce.Reducer*
* Define both input and output types to *Text* for the key (posteCode) and *LongWritable* for the values. If input and output types are the same, and if reducer can be applied to output again without changing result, then *Reducer* can also be used as *Combiner*.

## Implement *reduce()* method:
* It should counts total key (postCode) hits.
* It is aware that value pointed by iterator is re-used and as so **must** be copied (common pitfall).
* If *Reducer* is intended to be used as *Combiner* notice that counting loops in the iterator is not a solution. Input value should be added to a sub-total instead.

## Bootstrap your first Hadoop job
Complete PostcodeJob TODOs

## Validate
Run JUnit tests 

## **Optional**: run on a cluster
Complete lab1.sh TODOs, copy lab1.jar, store-domain.jar, customer.csv and lab1.sh on the cluster of your choice (for instance a Cloudera single node cluster running on a VM).