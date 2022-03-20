# -*- coding: utf-8 -*-
"""Crawler to collect tweets using twitter streaming API
   The streaming API can get tweets for last 7 seven days using Elevated access from twitter
   Run this script for 10-15 days to collect 1.5 to 2 gb of data.
"""

import tweepy as tw
import time
import warnings
import json
warnings.filterwarnings('ignore')

consumer_key="" #Enter your consumer key from Twitter Developer account here
consumer_secret="" #Enter your consumer secret from Twitter Developer account here
access_token="" #Enter your access token from Twitter Developer account here
access_token_secret="" #Enter your access token secret from Twitter Developer account here

auth=tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token=(access_token, access_token_secret)
api= tw.API(auth, wait_on_rate_limit= True, wait_on_rate_limit_notify=True)

query = "covid OR covid19 OR covid-19 OR nft OR crypto OR cryptocurrency OR cryptocurrencies OR blockchain OR bitcoin OR btc OR eth OR ethereum"
date_since = '2022-03-09'
date_until = '2022-03-10'

tweets = tw.Cursor(api.search,
                   q=query,
                   lang="en",
                   tweet_mode='extended',
                   since = date_since,
                   until = date_until).items(150000)

tweet_dict = {}

import re
def clean_tweets(text):
  text = re.sub("RT @[\w]*:","",text)
  text = re.sub("@[\w]*","",text)
  text = re.sub("https?://[A-Za-z0-9./]*","",text)
  text = re.sub("\n","",text)
  return text

while True:
    if len(tweet_dict)%3000 == 0:
      print(len(tweet_dict))
    try:
        tweet = tweets.next()
        temp_dict = {}
        temp_dict['id'] = tweet.id
        temp_dict['created_at'] = time.mktime(tweet.created_at.timetuple())
        temp_dict['hashtags'] = None
        temp_dict['screen_name'] = tweet.user.screen_name
        temp_dict['user_location'] = tweet.user.location
        temp_dict['place'] = None
        temp_dict['text'] = clean_tweets(tweet.full_text)
        if(tweet.place is not None):
          temp_dict['place'] = tweet.place.bounding_box.coordinates[0][0]
        if(tweet.entities is not None):
          if(tweet.entities['hashtags'] is not None):
            templist = []
            for ht in tweet.entities['hashtags']:
              templist.append(ht['text'])
            temp_dict['hashtags'] = templist

        tweet_dict[tweet.id] = temp_dict
    except tw.TweepError:
        time.sleep(60 * 15)
        continue
    except StopIteration:
        break

with open("cryptoTweets.json", "w") as outfile:
    json.dump(tweet_dict, outfile)