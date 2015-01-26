# Hadoop2
Some time ago, I was asked for an introduction course to Hadoop world. Here is the "hands on" part.

The projects in this repo aimed at a few targets:
* Help at real-life projects setup (pom hierarchy and projects granularity).
* Avoid dependencies headakes you invariably get when starting with hadoop (hadoop 1 / 2 incompatible APIs can put a mess in this domain).
* Introduce progressively different libraries, frameworks, practices and language I found usefull when writing hadoop jobs.

Feel free to report if it's of any use to you :smirk:

## Prerequisites
* JDK 1.8
* Hadoop 2 needs to be installed and HADOOP_HOME set for unit tests to run correctly. Linux / Mac OS users should refer to http://hadoop.apache.org for download and setup instructions. Windows users might visit https://github.com/ch4mpy/hadoop2/releases to get binaries and refer to https://wiki.apache.org/hadoop/Hadoop2OnWindows for instructions.
* Git to clone this repo.
* Minimum understanding of map/reduce.
* Provide a small lib to ease jobs unit testing (and initially to work around a bug, now corrected, on Windows).

## Eclipse setup
If you use Eclipse, you need to "import -> maven -> existing maven project". Further configuration (such as adding specific source folders to build path) will be provided in some labs instructions.

## Map / reduce
Hadoop jobs are a solution to process huge **collections of records**. Each job is executed in three steps:
* map: for each record you are provided with a key (defaulted to record index) and a value. You may associate this value (or a new one build from it) with any number ok keys (usually one). You do this implementing the Mapper interface.
* shuffle: all values that where mapped with the same key are collected in a single iterator. This is done automatically by the framework.
* reduce: you are provided with an iterator on all the values with a given key and may associate any number of key/value pairs to it (usually one single value with same output key as you got as an input). You do this with a Reducer implementation.

So Hadoop dev life is easy: compute keys from values and then compute new values from collections of records with same key. Of course life can get a slightly harder when it comes to compute what your client asks for with those two simple operations :bowtie:

## Business domain
All labs are manipulating very basic large distribution concepts:
* we sell products
* products are grouped by category
* each sell is materialized through a bill paid by a client

We will compute statistics about sales and customers.

![UML class diagram](/labs/store-domain/domain.png?raw=true)

## Get your hands on
The labs under /labs/todo/ are waiting for you. 

Each lab comes with a README with detailed instructions to help you find your way to the solutions provided under /labs/complete.
