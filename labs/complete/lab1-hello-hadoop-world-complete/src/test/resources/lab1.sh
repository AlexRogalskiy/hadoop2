hadoop fs -put input/customers.csv /user/gphd/input/customers.csv
hadoop fs -rmr /user/gphd/output
hadoop jar lab1-1.0.0-SNAPSHOT-jar-with-dependencies.jar /user/gphd/input /user/gphd/output
