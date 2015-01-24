REGISTER $PIGDIR/contrib/piggybank/java/piggybank.jar
REGISTER $PIGDIR/lib/*.jar

bills =
	LOAD '$input/bills.avro'
	USING org.apache.pig.piggybank.storage.avro.AvroStorage()
	AS (id:LONG, customerId:LONG, timestamp:LONG, productIds);

customers =
	LOAD '$input/customers.avro'
	USING org.apache.pig.piggybank.storage.avro.AvroStorage()
	AS (id:LONG, firstName:CHARARRAY, lastName:CHARARRAY, postcode:CHARARRAY);
	
products =
	LOAD '$input/products.avro'
	USING org.apache.pig.piggybank.storage.avro.AvroStorage()
	AS (id:LONG, label:CHARARRAY, price:DOUBLE, category);

productIdBill = FOREACH bills GENERATE id as billId, customerId, flatten(productIds) as productId;
productBill = JOIN productIdBill by productId, products by id;
customerProductBill = JOIN customers by id, productBill by productIdBill::customerId;
poscodeCategoryTurnover = FOREACH customerProductBill GENERATE customers::postcode as postcode, productBill::products::category as category, productBill::products::price as amount;
grp = GROUP poscodeCategoryTurnover BY (postcode, category);
result = FOREACH grp GENERATE group.postcode as postcode, group.category.label as category, SUM(poscodeCategoryTurnover.amount) as turnover;

STORE result 
	INTO '$output'
	USING org.apache.pig.piggybank.storage.avro.AvroStorage();
	