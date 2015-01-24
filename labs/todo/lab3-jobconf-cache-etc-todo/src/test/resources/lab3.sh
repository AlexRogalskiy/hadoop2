hadoop fs -put input/customers.csv /user/gphd/input/customers.csv
hadoop fs -put referential/fr_urban_postcodes.txt /user/gphd/referential/fr_urban_postcodes.txt
hadoop fs -rmr /user/gphd/output
hadoop jar lab3-1.0.0-SNAPSHOT.jar /user/gphd/referential/fr_urban_postcodes.txt /user/gphd/input /user/gphd/output
