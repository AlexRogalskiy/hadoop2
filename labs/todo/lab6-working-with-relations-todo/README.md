# Lab6: join records from heterogenous collections
**Target:** Get a picture of how to chain jobs and join heterogenous collections to get clients expectations done.
Here we do this parsing collections one after the other, filling intermediate structure step by step.
An other way do achieve the same goal is to parse all collections at once (using Avro *Schema.createUnion* capability for input) and testing for what data is available in each input record (quite dirty to me).

**Start point:** All tests and properties files are provided. You need to create mappers, reducers and jobs configuration.

Please note you can disable end to end test by commenting *PostcodeCategoryTurnoverJobTest* @Test annotation. 
Doing so while you develop mappers and reducers will fasten dramatically JUnit runs and save you great time validating each of your steps.

## Jobs mappers & reducers
1. *Bill2PostcodeCategoryTurnoverTmpMapper* should create *PostcodeCategoryTurnoverTmp* for each *productId* in *SerializableBill* (feeds *customerId* and *productId*)
2. *Customer2PostcodeCategoryTurnoverTmpMapper* should create *PostcodeCategoryTurnoverTmp* for each *SerializableCustomer* (feeds *customerId* and *postcode*)
3. *Product2PostcodeCategoryTurnoverTmpMapper* should create *PostcodeCategoryTurnoverTmp* for each *SerializableProduct (feeds *productId*, *category* and *amount*)
4. *PostcodeCategoryTurnoverTmpByCustomerIdMapper* should index *PostcodeCategoryTurnoverTmp* by *customerId*.
5. *PostcodeCategoryTurnoverTmpByCustomerIdReducer* should merge *PostcodeCategoryTurnoverTmp* sharing the same customerId (fills customerId, postcode and productId but keeping products multiplicity.
6. *PostcodeCategoryTurnoverTmpByProductIdMapper* should index *PostcodeCategoryTurnoverTmp* by productId.
7. *PostcodeCategoryTurnoverTmpByProductIdReducer* should merge *PostcodeCategoryTurnoverTmp* sharing the same productId (fills customerId, postcode, productId, category and amount).
8. *PostcodeCategoryTurnoverTmpByPostcodeCategoryMapper* should used now filled *PostcodeCategoryTurnoverTmp* to create *PostcodeCategoryTurnover* matching a key composed of post-code and product category (for instance by appending the second to the first).
9. *PostcodeCategoryTurnoverReducer* will finally sum the amounts for each key

## Configure the jobs chain
Complete the job-context TODOs to have a complete process