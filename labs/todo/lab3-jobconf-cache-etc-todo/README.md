# Lab3: Tip and tricks
**Target:** code re-use, job configuration and local cache

**Start point:** working project to refactor

The names *PostcodeMapper* and *PostcodeReducer* are good sign it wasn't made to be re-used. 

If we read mapper code, we can notice that it would work each time we need to extract the second field of a CSV record and collect it as key. 
If we refactor it to read from the configuration the index of the field to extract it could be even more easy to re-use.

The reducer is already pretty generic: it can be used each time we need to sum long values associated with a key. 
It should, at a bare minimum, be renamed to something like *LongSumReducer* and, in a larger project, be moved to a shared library.

As a demo for distributed cache use, we will modify business requirements: 
we no longer want to count post-codes occurences, but want to count "non French", "french-urban" or "french-rural" records.
You are provided with a reference file listing french-urban postcode patterns, which should be put to and retrieved from the distributed cache.

## Use existing components when possible
Trash the reducer to use *org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer* instead

## Use job configuration and distributed cache
Follow *CsvFieldCountMapper* and *CsvFieldCountMapperTest* TODOs
