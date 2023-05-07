#!/bin/bash

# Set the environment variables for Spark and Hive
export SPARK_HOME=/opt/spark2
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
 

hdfs dfs -rm -r /data/checkpoint
hdfs dfs -rm -r /twitter-landing-data/_spark_metadata

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --conf spark.pyspark.python=/usr/bin/python3 --queue streaming stream.py > /dev/null 2>&1 &

python3 /home/itversity/itversity-material/twitter_listener.py

