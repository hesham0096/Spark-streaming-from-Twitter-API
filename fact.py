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

spark.sql("Create External TABLE IF NOT EXISTS fact_processed( \
  author_id string, \
  username string, \
  followers integer, \
  followings integer, \
  verified string,  \
  number_Of_Tweets integer , \
  Retweets integer,  \
  favorites integer, \
  replies integer \
  ) \
STORED AS PARQUET  \
Location '/twitter-processed-data'") 

spark.sql("INSERT OVERWRITE TABLE fact_processed \
          SELECT DISTINCT \
          u.author_id , \
		  username,\
		  followers , \
		  followings , \
		verified,\
		tweets_number , \
		sum(retweets) OVER (PARTITION BY t.author_id), \
        sum(favorites) OVER (PARTITION BY t.author_id), \
        sum(replies) OVER (PARTITION BY t.author_id) \
          FROM user_raw u join tweet_raw t \
          on u.author_id= t.author_id ")