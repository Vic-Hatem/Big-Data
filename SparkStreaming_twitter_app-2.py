#!/usr/bin/env python
# coding: utf-8

# # Hatem Khater-209002799
# # Moran Totry-209518018
# # Alaa Safadi-318461019

# # Spark Streaming Twitter Example - parse the stream
# 
# Based upon https://www.toptal.com/apache/apache-spark-streaming-twitter
# 
# ### Preparations
# 
# 1. In the Spark_streaming_twitter_app-1 notebook start the tcp socket streaming from twitter
# 2. Copy the Hashtags directory from 
# 3. Edit the file named app.py in TwitterStreaming/HashtagsDashboard and change the host from `localhost` to `0.0.0.0`
# 4. Open a firewall rule (https://console.cloud.google.com/networking/firewalls/list) to allow from all ips (0.0.0.0/0) to access *tcp* on port **5001**.
# 5. Run the dashboard app by running the command `python app.py`
# 6. Navigate using Edge/Internet Explorer/Firefox to http://your.ip.add.ress:5001 , e.g. http://35.204.240.235:5001/
# 
# ### Imports and connection

# In[ ]:


#### from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

# create the Streaming Context from the above spark context with interval size 4 seconds
ssc = StreamingContext(sc, 1)

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9009
dataStream = ssc.socketTextStream("localhost",9009)


# ### Define a function to retrieve the sql context and convert the streaming rdd to a dataframe

# In[ ]:


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']
d1=[]
def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(tweet=w))
        # create a DF from the Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        hashtags_df.registerTempTable("tweets")
        # get the top 10 hashtags from the table using SQL and print them
        hashtag_counts_df = sql_context.sql("select * from tweets")
        l1=list(hashtag_counts_df.select('tweet').toPandas()['tweet'])
        d1.append(str(l1))
    except:
        e = sys.exc_info()[0]


# 

# In[ ]:


## Here We check if the tweet includes any "lone" or "alone" substring
def filter_tweets(tweet):
    if(tweet!=None and ((tweet.find(" lone")!=-1) or (tweet.find(" alone")!=-1))):
        return True
    else:
        return False

tweetss = dataStream.flatMap(lambda t:t.split('\n'))
line = tweetss.filter(lambda l:filter_tweets(l))
# # do processing for each RDD generated in each interval
line.foreachRDD(process_rdd)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
## Here we were changing according to how many tweets we want to recieve .
# for 10,000 = awaitTermination(1200)
# for 10,000 = awaitTermination(12000)
ssc.awaitTermination(12000)
print("stopping")


# In[ ]:


import pandas as pd
import csv
num=800
dictCSV=pd.read_csv('lonely_lex.csv')
usernamesList=[]
usertweets={}
array=[]
d={}
threshold=0
for x,y in dictCSV.iterrows():
    array.append(y['term'])
    d[y['term']]=y['weight']

valuestmp=[]
checkedword=[]
tweet_tuple=[]


for tweet in d1:
    sum=0
    b={}
    print(tweet)
    if(tweet[tweet.find("@"):tweet.find(" ")] !=""):
        username=tweet[tweet.find("@"):tweet.find(" ")]
        print(username)
    if(username not in usernamesList):
        usernamesList.append(username)
    wordsArray=tweet.split()
    for word in wordsArray:
        if(word in array):
            checkedword.append(word)
    if(username in usertweets.keys()):
        valuestmp=usertweets[username]+checkedword
        usertweets[username]=valuestmp
    else:
        usertweets[username]=list(checkedword)
        checkedword.clear()
        
# A:tweets lonely level
# Here we return CSV file with tweets and their level (Descending) 
#then after getting the CSV file we looked at it to try to find the Threshold
## answering this part : "דרגו את הטוויטים בסדר יורד לפי ציון הבדידות שלהם"
    for words in usertweets[username]:
        b[words]=round(usertweets[username].count(words)/len(usertweets[username]),3)
        sum+=b[words]*d[words]
        tweet_tuple.append((tweet,sum))
tweet_tuple.sort(reverse=True,key=lambda x:x[1])
filename='tweet-lonely-level.csv'
with open(filename, "w") as f:
    writer = csv.writer(f , lineterminator='\n')
    for t in tweet_tuple:
        writer.writerow(t)
f.close()


#B:users lonely level
#after running 10k tweets we figured that threshold should be 0 it seems like
#the dictionary (lonely_lex) contains terms with a negative weight, that may be
#represent word that is not relevant and serious to being lonely!
## answering this part : "בחרו ציון סף בדידות שמחזיר כמות קטנה ככל האפשר של אנשים שלא באמת הביעו בדידות"

tuples=[]
threshold=0
for key in usertweets:
    a={}
    sum=0
    for part in usertweets[key]:
        a[part]=round(usertweets[key].count(part)/len(usertweets[key]),3)
        sum+=a[part]*d[part]
    if sum>=threshold:
        tuples.append((key,sum))
tuples.sort(reverse=True,key=lambda x:x[1])
filename='usr-lonely-level.csv'
with open(filename, "w") as f:
    writer = csv.writer(f , lineterminator='\n')
    for t in tuples:
        writer.writerow(t)
f.close()
ssc.stop()


# In[1]:


## We used as reference: 
## https://www.toptal.com/apache/apache-spark-streaming-twitter
## https://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams
## https://mungingdata.com/pyspark/column-to-list-collect-tolocaliterator/
## Machine learning lectures 
## Machine Learning labs


# In[ ]:





# In[ ]:





# In[ ]:




