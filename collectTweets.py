import tweepy
import os
from Connection import getConnectionObj
import json
from dotenv import dotenv_values
from pymongo import MongoClient

# from ApiKeys import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET

config = dotenv_values(".env")
print(f"Read the mongo connection details from .env file")

consumerKey = config["API_KEY"]
consumerSecret = config["API_KEY_SECRET"]
accessToken = config["ACCESS_TOKEN"]
accessTokenSecret = config["ACCESS_TOKEN_SECRET"]
bearerToken = config["BEARER_TOKEN"]

mongodb_client = MongoClient(config["ATLAS_URI"])
print("Mongo client created")

def get_database():
    return mongodb_client[config["DB_NAME"]]

def get_collection_name(dbname):
    return dbname[config["TWEET_COLLECTION"]]

def get_chart_collection_name(dbname):
    return dbname[config["CHART_COLLECTION"]]

def collectTweetForTopic(track,artist):
    # Authenticate with Twitter API
    auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessTokenSecret)
    api = tweepy.API(auth)
    connection = getConnectionObj()
    cursor = connection.cursor()
    # Search for tweets containing a hashtag
    topic = track+" AND "+artist
    tweets = tweepy.Cursor(api.search_tweets, q=topic).items(30)
    collection_name = get_collection_name(dbname)
    for tweet in tweets:
    #     cursor.execute('INSERT INTO tweets (topic, tweet_id, tweet) VALUES (%s, %s, %s)',
    #                             (topic, tweet.id ,tweet.text))
    #     connection.commit()
        print(tweet.text)
        data = {'track':track,
                'artist':artist,
                'tweet':tweet.text}
        document = collection_name.insert_one(data)
        cursor.close()
        connection.close()

dbname = get_database()
chart_collection = get_chart_collection_name(dbname)
chart = chart_collection.find({})
if(chart):
    for doc in chart:
        track = doc["track"]
        track_name = track["name"]
        artists = track["artists"]
        artist_name = artists[0]["name"]
        collectTweetForTopic(track_name,artist_name)
# collectTweetForTopic("Search & Rescue","Drake")

