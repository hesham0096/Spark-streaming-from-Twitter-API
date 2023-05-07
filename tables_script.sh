#!/bin/bash

# Set the environment variables for Spark and Hive
export SPARK_HOME=/opt/spark2
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive

# Hive script
$SPARK_HOME/bin/spark-sql --master yarn --conf spark.sql.shuffle.partitions=5 -f /home/itversity/all_data.sql
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --conf spark.pyspark.python=/usr/bin/python3 fact.py 
