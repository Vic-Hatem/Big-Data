import pandas as pd
import pyarrow
import csv

num = 800
dictCSV = pd.read_csv('gs://hatems_bucket/lonely_lex.csv')
usernamesList = []
usertweets = {}
array = []
d = {}
for x, y in dictCSV.iterrows():
    array.append(y['term'])
    d[y['term']] = y['weight']

for i in range(25):
    strings = 'gs://twitter-stress/twitter.parquet/part-00{}'.format(
        num) + '-ec74b3d9-8fda-45f0-b423-dba02adeb11c-c000.snappy.parquet'
    num += 1
    print(num - 800)
    df = pd.read_parquet(strings, engine='pyarrow')
    counter = 0
    valuestmp = []
    checkedword = []
    for i, j in df.iterrows():
        tweet = j['body']
        if (tweet != None and ((tweet.find(" lone") != -1) or (tweet.find(" alone") != -1))):
            counter += 1
            if (tweet[tweet.find("@"):tweet.find(" ")] != ""):
                username = tweet[tweet.find("@"):tweet.find(" ")]
                if (username not in usernamesList):
                    usernamesList.append(username)
                wordsArray = tweet.split()
                for word in wordsArray:
                    if (word in array):
                        checkedword.append(word)
                if (username in usertweets.keys()):
                    valuestmp = usertweets[username] + checkedword
                    usertweets[username] = valuestmp
                else:
                    usertweets[username] = list(checkedword)
                checkedword.clear()

tuples = []
for key in usertweets:
    a = {}
    sum = 0
    for part in usertweets[key]:
        a[part] = round(usertweets[key].count(part) / len(usertweets[key]), 3)
        sum += a[part] * d[part]
    tuples.append((key, sum))
tuples.sort(reverse=True, key=lambda x: x[1])
filename = 'HW2.csv'
with open(filename, "w") as f:
    writer = csv.writer(f, lineterminator='\n')
    for t in tuples:
        writer.writerow(t)
f.close()