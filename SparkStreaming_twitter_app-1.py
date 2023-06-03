#!/usr/bin/env python
# coding: utf-8

# # Spark Streaming Twitter Example - create the stream
# 
# Based upon https://www.toptal.com/apache/apache-spark-streaming-twitter
# 
# ### Preparations
# 
# 1. In another console install requests_oauthlib and flask by executing this command: `pip install requests_oauthlib flask`
# 2. Create a twitter app on https://apps.twitter.com/
# 3. In your App home-page under the Keys and tokens tab press **create** to create new access tokens. 
# 
# ### Imports and tokens
# 
# **Make sure to replace the tokens with your tokens**

# In[ ]:


import socket
import sys
import requests
import requests_oauthlib
import json
tweet_counter=0
# Replace the values below with yours
# ACCESS_TOKEN = '1297823647185469440-abKvn4NNex4OMuC9ZYAXdXdt3ZcdkH'
# ACCESS_SECRET = 'P05TMKshZmxtoARIOL2sXc6pBv2xvtlfA9Z3mwDpPD321'
# CONSUMER_KEY = 'rD7v3aTLPj3qMvVsZGOgeZqAg'
# CONSUMER_SECRET = 'f11SXKqa59CVtWeopuA5Byp6fv5iOKOXRm5cFOIYEaV33VozgW'

ACCESS_TOKEN = '1343287203422740480-bQJs7ai2gOJRWQUigp0oL5jkppXJW0'
ACCESS_SECRET = 'VvRsiG3MGUXdLBzNGncmIr4jimbDFEnGlPlVlaQhAtCb0'
CONSUMER_KEY = 'w9Kc4bw8fnTyqKYTeR68kHqSa'
CONSUMER_SECRET = 'spzniQWZg8TEaCr4iG8Vmi7QIoTBsMOsxy7fT0X6o1t70oqY8k'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


# ### Define a function that connects to Twitter API and  and gets tweets

# In[ ]:


def get_tweets():
    
    query_dataT = [('language', 'en'),('locations', '-130,-20,100,50'),('track','#')]
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    # Will fetch all the hashtags written in the location stated in English
    query_data = [('language', 'en'),('locations', '-130,-20,100,50'),('track','#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


# ### Define a function that streams the tweets over a tcp connection

# In[ ]:


def send_tweets_to_spark(http_resp, tcp_connection,tweet_counter):
    text = ''
    for line in http_resp.iter_lines():
        if tweet_counter==100000:
            break
        try:
            full_tweet = json.loads(line)
            tweet_text = str(full_tweet['text'])
            username=str(full_tweet['user']['screen_name'])
            urls=full_tweet['entities']['urls']
            for url1 in urls:
                title=url1["unwound"]["title"].encode("utf-8")
                url=url1["url"].encode("utf-8")
                text=text.replace(url,title)
                tweet_text=text   
            temp="@"+username+" : "+tweet_text+'\n'
            temp=temp.encode('utf-8')
            tweet_counter+=1
            print(temp)
            tcp_connection.send(temp)
        except KeyError:
            e = sys.exc_info()[0]
            


# ### Start a localhost tcp socket to stream the tweets

# In[ ]:


TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn,tweet_counter)


# ### Open notebook SparkStreaming_twitter_app-2, and run it to accept the tweets 

# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




