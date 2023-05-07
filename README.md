# Spark-streaming-from-Twitter-API
Spark-steraming App using pyspark, Python, Hive
This project is divided into multiple phases 

Step one (Twitter_listener.py)
•	Wrote a script code to get the desired attribute from twitter API which were tweet_id , tweet_time , tweet_text, author_id, username, verified or not , 
 number of retweets, number of likes and number of replies and some other parameters belongs to the user’ account.
•	To keep the streaming, I had to put the code inside an infinite loop knowing that My starting time is 5 minutes before the current time. 

Step Two (Stream.py)
•       From spark end I had to establish a streaming connection on the same port, also to enable Hive services, drop duplicates and parsing the parameters 
        into columns.
•	Wrote the streamed data into a partitioned parquet file then saved it into the desired directory which was /twitter-landing-data. 


Step Three (all_data.sql)
•	Simply Created an external hive table its location was the directory /twitter-landing-data  and of course it had to be partitioned to be able to read the 
       data from the directory.

Step Four (all_data.sql)
•	With the same logic created two dimensions which were user under the table user_raw and tweet under the table tweet_raw, -I tried to get location but 
        most of the result was null, so I decided not to take it into consideration-
•	Both of the tables were external and has locations /twitter-raw-data/user_raw,
       /twitter-raw-data/tweet_raw.
•	I choose external table as I see it’s safer for the data to be saved in an external directory so if the tables were removed by any chance the data would 
        be safe. 

Step Five(all_data.sql)
•	To Create the Fact table I choose the granularity level to be the User, and It made sense to have most of the important parameter that is in the user 
        table. 
•	To get the number of tweets per user I had to join the dimensions on the user_id then insert the output into the fact_processed table.
•	To add some extra parameters I choose to add the count of likes for each user all over his tweets that got retrieved using window functions 
•	With the same logic I got the sum of replies, retweets and favorites for each user all over his retrieved tweets.

Step Six(stream.sh, tables_script.sh)
•	To Orchestrate all of these script, I preferred to divide it into two bash scripts, the first one which is strem.sh responsible of running only two scripts 
        twitter_listener and strem.py which are responsible of streaming, stream.py will run on background but I left twitter_listener to run on screen to check 
        that the streaming is working fine by just checking the terminal
•	On another terminal and since a new data comes each 5  minutes I choose to put tables_script.sh which has the creation and insertion statements of 
       the fact and dimensions to be put on crontab and scheduled to run each 5 minutes.
