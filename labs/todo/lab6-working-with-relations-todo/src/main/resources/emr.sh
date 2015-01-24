ruby elastic-mapreduce --create --name lab --alive --log-uri s3n://hadoop-labs-s3-bucket/log/lab/
# --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop --args "--site-config-file,s3://hadoop-labs-s3-bucket/bootstrap/emr-settings.xml"
scp -i c:\dev\elastic-mapreduce-ruby\ch4mp_aws_key.pem .\lab6-working-with-relations-complete\target\lab6-1.0.0-SNAPSHOT.jar hadoop@ec2-54-229-69-74.eu-west-1.compute.amazonaws.com:/home/hadoop/job/lab6-1.0.0-SNAPSHOT.jar
ssh -i c:\dev\elastic-mapreduce-ruby\ch4mp_aws_key.pem hadoop@ec2-54-229-69-74.eu-west-1.compute.amazonaws.com
java -cp "/home/hadoop/*:/home/hadoop/lib/*:/home/hadoop/job/lab6-1.0.0-SNAPSHOT.jar" com.c4soft.hadoop.Main
