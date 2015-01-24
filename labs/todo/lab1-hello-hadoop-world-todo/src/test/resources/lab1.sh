#hadoop binary offers quite a lot of options. What you need here are "fs" and "jar" options

#TODO copy input files to HDFS
hadoop fs -put LOCAL_PATH HDFS_PATH

#TODO delete output folder
hadoop fs -rmr HDFS_PATH

#TODO lunch lab1. Do not forget to declare dependency on store-domain jar.
hadoop jar JAR_NAME MAIN_CLASS -libjars NON_STD_LIBS ARGS...
