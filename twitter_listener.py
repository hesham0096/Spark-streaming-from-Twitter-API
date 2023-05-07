# For sending GET requests from the API
import requests

# For saving access tokens and for file management when creating and adding to the dataset
import os

# For dealing with json responses we receive from the API
import json

# For displaying the data after
import pandas as pd

# For saving the response data in CSV format
import csv

# For parsing the dates received from twitter in readable formats
import datetime
import dateutil.parser
import unicodedata
from datetime import datetime, timedelta


#To add wait time between requests
import time

#To open up a port to forward tweets
import socket 

os.environ['TOKEN'] =  "AAAAAAAAAAAAAAAAAAAAAIdCcgEAAAAADxqlmxSiZLO05fKmfbrX7G3ckqQ%3DCCPSGoWTDF6uu4qdFsncsuOat5GFTTFv5blXPdA4ueK4YLu3gg"
# AAAAAAAAAAAAAAAAAAAAAIdCcgEAAAAAi08uZ2TTsmCCm3%2BSu9zICPXESiA%3DGc60t2rmLpn4Nbu48EE5SAL718SIZeDc0NpVytEIPMyxXFSqNM
def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)


def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "Messi lang:en"

max_results = 100

s = socket.socket()
host = "127.0.0.1"
port = 7777
s.bind((host, port))
print("Listening on port: %s" % str(port))
s.listen(5)
clientsocket, address = s.accept()
print("Received request from: " + str(address)," connection created.")

while True: 
    start_time = (datetime.utcnow() - timedelta(minutes=5) ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    end_time = (datetime.utcnow() - timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    url = create_url(keyword, start_time, end_time, max_results)
    json_response = connect_to_endpoint(url[0], headers, url[1])
   
    users_dict = {user['id']: user for user in json_response['includes'].get('users', [])}
    users_stats = {user['id']: {'follower_count': user['public_metrics']['followers_count'],
                          'following_count': user['public_metrics']['following_count'],
                          'tweet_count': user['public_metrics']['tweet_count']}
              for user in json_response['includes'].get('users', [])}

    for data in json_response['data']:
        tweet_text = data['text']
        tweet_time = data['created_at']
        tweet_id = data['id']
        author_id = data['author_id']
        likes= data['public_metrics']['like_count']
        retweets= data['public_metrics']['retweet_count']
        replies= data['public_metrics']['reply_count']



        # Get the relevant user info based on author_id
        author_info = users_dict.get(author_id, {})
        username = author_info.get('username')
        verified = author_info.get('verified')
        author_stats = users_stats.get(author_id, {})
        follower_count = author_stats.get('follower_count')
        following_count = author_stats.get('following_count')
        tweet_count = author_stats.get('tweet_count')


        

        message = {'tweet_id': tweet_id, 'tweet_time': tweet_time, 'tweet_text': tweet_text, 'author_id': author_id, 'username': username,               'verified': verified, 'retweets': retweets, 'replies': replies, 'favorites': likes , 
         'followers': follower_count, 'followings':following_count , 'tweets':tweet_count  }
        
        message_json = json.dumps(message)
        print("Sending:", message_json.encode('utf-8'))
        u = message_json + '\n'
        clientsocket.send(u.encode('utf-8'))

    time.sleep(120)
