CREATE EXTERNAL TABLE IF NOT EXISTS all_data (
  tweet_id STRING,
  tweet_time STRING,
  tweet_text STRING,
  author_id STRING,
  username STRING,
  verified STRING,
  followers INT,
  followings INT,
  tweets_number INT,
  retweets INT,
  replies INT,
  favorites INT
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/twitter-landing-data';

MSCK REPAIR TABLE all_data;

CREATE TABLE IF NOT EXISTS user_raw(
  author_id string,
  username string,
  verified string,
 followers integer, 
 followings integer, 
 tweets_number integer 
)
PARTITIONED BY (year int , month int , day int , hour int)
STORED AS PARQUET
Location '/twitter-raw-data/user_raw';

MSCK REPAIR TABLE user_raw;


CREATE TABLE IF NOT EXISTS tweet_raw(
 author_id string,
  tweet_id string,
  tweet_text string,
  retweets integer,
  replies integer, 
 favorites integer 
)
PARTITIONED BY (year int , month int , day int , hour int)
STORED AS PARQUET
Location '/twitter-raw-data/tweet_raw';

MSCK REPAIR TABLE tweet_raw;


SET hive.exec.dynamic.partition.mode=nonstrict;




INSERT OVERWRITE TABLE user_raw PARTITION (year, month, day, hour) 
SELECT DISTINCT  
          author_id, 
          username, 
          verified, 
          followers, 
          followings, 
          tweets_number, 
          year, 
          month, 
          day, 
          hour 
FROM all_data;

MSCK REPAIR TABLE user_raw;


INSERT OVERWRITE TABLE tweet_raw PARTITION (year, month, day, hour) 
SELECT DISTINCT 
		  author_id,
          tweet_id, 
          tweet_text, 
          retweets, 
          replies , 
          favorites , 
          year, 
          month, 
          day, 
          hour 
FROM all_data;

MSCK REPAIR TABLE tweet_raw;