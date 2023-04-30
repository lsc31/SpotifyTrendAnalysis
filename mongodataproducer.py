from kafka import KafkaProducer
import os
import json
from pymongo import MongoClient
import argparse
import time

mongodb_client = MongoClient("mongodb+srv://<username>:<password>@cluster0.mwzfmxm.mongodb.net/?retryWrites=true&w=majority")
# print("Mongo client created")

def get_database():
    return mongodb_client["spotifycharts"]

def get_collection_name(dbname):
    return dbname["tweets2"]

def get_chart_collection_name(dbname):
    return dbname["chartUS50"]

def get_analyzedtweet_collection(dbname):
    return dbname["analyzeddata"]

dbname = get_database()
chart_collection = get_chart_collection_name(dbname)
chart = chart_collection.find({})
track_list = []
if(chart):
    for doc in chart:
        track_obj = doc["track"]
        track_list.append(track_obj["name"])

parser = argparse.ArgumentParser(description="Producer arguments",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
help_msg = "Provide track name from the list: \n" + str(track_list)
parser.add_argument("track", type=str, help=help_msg)
args = parser.parse_args()
config = vars(args)
search_term = config['track']
print("Track to analyze:" + search_term)

# search_term = "Last Night"
topic_name = 'twitter'


class TweetListener():

    def start_streaming_tweets(self, search_term, collection_name):
        raw_tweets = collection_name.find({"track":search_term})
        if(raw_tweets):
            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            for t in raw_tweets:
                print(t)
                producer.send(topic_name, value=json.dumps(t,default=str).encode('utf-8'))
            producer.close()
        else:
            print("No records found")
        
def compute_metric(tweets,count):
    pos=0
    neg=0
    neu=0
    for tweet in tweets:
        if tweet['svm_sentiment']=='Positive':
            pos=pos+1
        elif tweet['svm_sentiment']=='Negative':
            neg=neg+1
        else:
            neu=neu+1
    print("Analysis Result:\n")
    print("Positive: "+str("{:.2f}".format(pos/count*100)))
    print("Negative: "+str("{:.2f}".format(neg/count*100)))
    print("Neutral: "+str("{:.2f}".format(neu/count*100)))

def viewSentimentMetric():
    analyzed_collection = get_analyzedtweet_collection(dbname)
    analyzed_tweets = analyzed_collection.find({"track":search_term})
    count = analyzed_collection.count_documents({"track":search_term})

    if count > 0:
        compute_metric(analyzed_tweets,count)
    else:
        print("No tweets to analyze yet")
        max_retries = 0
        while count == 0 and max_retries < 3:
            time.sleep(2)
            analyzed_tweets = analyzed_collection.find({"track":search_term})
            count = analyzed_collection.count_documents({"track":search_term})
            max_retries += 1
        if max_retries >= 3:
            print("Maximum retries reached. Exiting.")
        else:
            compute_metric(analyzed_tweets,count)

if __name__ == '__main__':
    # dbname = get_database()
    collection_name = get_collection_name(dbname)
    twitter_stream = TweetListener()
    twitter_stream.start_streaming_tweets(search_term, collection_name)
    viewSentimentMetric()
