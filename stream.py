# Import your dependecies
import pyspark # run after findspark.init() if you need it
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from os.path import abspath
from pyspark.sql.functions import split
from pyspark.sql.functions import year, month, dayofmonth, hour, split
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("TwitterStream") \
   .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()


tweet_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 7777) \
    .load() 
    
tweet_df_string = tweet_df.selectExpr("CAST(value AS STRING)")
 
df1=tweet_df_string.withColumn("tweet_id",get_json_object(col("value"), "$.tweet_id"))\
.withColumn("tweet_time",get_json_object(col("value"), "$.tweet_time"))\
.withColumn("tweet_text",get_json_object(col("value"), "$.tweet_text"))\
.withColumn("author_id",get_json_object(col("value"), "$.author_id"))\
.withColumn("username",get_json_object(col("value"), "$.username"))\
.withColumn("verified",get_json_object(col("value"), "$.verified"))\
.withColumn("retweets",(get_json_object(col("value"), "$.retweets")).cast("integer"))\
.withColumn("replies",(get_json_object(col("value"), "$.replies")).cast("integer"))\
.withColumn("favorites",(get_json_object(col("value"), "$.favorites")).cast("integer"))\
.withColumn("followers",(get_json_object(col("value"), "$.followers")).cast("integer"))\
.withColumn("followings",(get_json_object(col("value"), "$.followings")).cast("integer"))\
.withColumn("tweets_number",(get_json_object(col("value"), "$.tweets")).cast("integer"))


df1 = df1.drop(col("value"))
df1 = df1.dropDuplicates(["tweet_text"])

df1 = df1.withColumn('year', year('tweet_time')) \
       .withColumn('month', month('tweet_time')) \
       .withColumn('day', dayofmonth('tweet_time')) \
       .withColumn('hour', hour(split('tweet_time', 'T')[1]))
       
       
       
       
writeTweet = df1.coalesce(1).writeStream. \
    outputMode("append"). \
    format("parquet"). \
    partitionBy("year","month", "day", "hour"). \
    option("path" , "/twitter-landing-data"). \
    option("checkpointLocation" , "/data/checkpoint"). \
    start()

writeTweet.awaitTermination()