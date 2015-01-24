# Hadoop2
A set of labs and tools to get you started into hadoop development world

## Prerequisites
* JDK 1.8
* Hadoop 2 needs to be installed and HADOOP_HOME set for unit tests to run correctly. Please refer to http://hadoop.apache.org/ for download and setup instructions.
* Git to clone this repo

## Eclipse setup
If you use Eclipse, you need to "import -> maven -> existing maven project", select all projects from this repo, and add following "source folders" to some lab projects:
* src/main/avro
* target/generated-sources
* src/main/pig
Please note that you may need to run `mvn install` to generate Java classes (in target/generated-sources) from Avro schemas (in src/main/avro)
