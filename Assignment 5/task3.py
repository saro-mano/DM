import random
import sys
import tweepy
import time
from pyspark import SparkContext
import pyspark
from tweepy import Stream, StreamListener

sc = pyspark.SparkContext('local[*]')
sc.setLogLevel("ERROR")

port_number = int(sys.argv[1])
output_file = sys.argv[2]

api_key = 'UOEpDUA50fgrmPSGlSdamdknd'
api_key_secret = 'kkXP4KiS3bGGm6eqgJT7OuVoZJVNVJx1fyAoZpKs0v1E1mGqLS'
access_token = '2959258688-AKjlyTxuHqh7AygmS9bN5cEldk25bwXZkabciGQ'
access_token_secret = 'H7hKFpB88M83yHKMbyo9GjcByjzeYKORdvBS8LriC3ORH'

sequence_number = 0
tweet_list = list()

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        global tweet_list
        global sequence_number
        hashtags = status.entities.get("hashtags")
        if len(hashtags) != 0:
            sequence_number = sequence_number + 1
            if sequence_number < 101:
                tweet_list.append(status)
            else:
                tweet_list[random.randint(0, 100)] = status
            get_output()

def isEnglish(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True

def get_output():
    rdd = sc.parallelize(tweet_list).flatMap(lambda x: x.entities.get("hashtags")).map(lambda x: (x["text"], 1)).filter(lambda x: isEnglish(x[0])).reduceByKey(lambda x, y: x+y)
    top = sorted(rdd.map(lambda x: x[1]).distinct().collect(), reverse=True)[:3]
    tag_with_number = rdd.filter(lambda x: True if x[1] in top else False).sortBy(lambda x: (-x[1], x[0])).collect()
    # print(tag_with_number)
    if sequence_number == 1:
        file = open(output_file, "w+")
    else:
        file = open(output_file, "a+")
    file.write('The number of tweets with tags from the beginning: {}\n'.format(sequence_number))
    for i in tag_with_number:
        file.write('{} : {}\n'.format(i[0], i[1]))
    file.write('\n')

auth = tweepy.OAuthHandler(consumer_key=api_key, consumer_secret=api_key_secret)
auth.set_access_token(key=access_token, secret=access_token_secret)
api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
myStream.filter(track=["election", "result", "covid", "biden", "trump", "california"])