# Lab2: tests
**Target:** write automated tests for Hadoop mappers, reducers and jobs. 

**Start point:** about the opposite situation compared to lab1: Mapper and Reducer are provided, but tests are not.

## PostcodeMapper tests:
Complete *PoscodeMapperTest* TODOs.

## Create PostcodeReducerTest:
* Right click *PostcodeReducer* and select *New → Junit Test Case*
* choose *src/test/java* as destination folder
* select *setUp()* generation
* select *reduce()* test template generation
* implement setUp and reduce test bodies like it is done for the mapper 

## Create a PostcodeMRTest:
This class doesn't aim at testing a specific class but the chaining of a Mapper and a Reducer with lunching the full Hadoop stack (which takes a while).
* Instantiate a MapReduceDriver.
* Feed it with the lab mapper and reducer (which can also be used as combiner)

## Create PostcodeJobTest
This test will launch the full Hadoop stack. As a consequence it will take much longer to run. Maybe once you have experienced how much longer, you will think twice before doing the same in your every-day projects...

* Your test class should extend *Junit4ClusterMapReduceTestCase*
* Take some time to understand what this Junit4ClusterMapReduceTestCase* class does (mini-cluster startup and shutdown, output directories cleanup, etc.)
* you should test that the job:
  - actually returns 0
  - output file actually contains 2 lines
  - those line actually contain expected key/value pairs separated by a tab

## Run your tests
Get the green bar!