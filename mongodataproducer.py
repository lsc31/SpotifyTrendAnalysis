from kafka import KafkaProducer
import os
import json
from pymongo import MongoClient
import argparse

mongodb_client = MongoClient("mongodb+srv://<username>:<password>@cluster0.mwzfmxm.mongodb.net/?retryWrites=true&w=majority")
# print("Mongo client created")

def get_database():
    return mongodb_client["spotifycharts"]

def get_collection_name(dbname):
    return dbname["tweets2"]

def get_chart_collection_name(dbname):
    return dbname["chartUS50"]

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
        

if __name__ == '__main__':
    # dbname = get_database()
    collection_name = get_collection_name(dbname)
    twitter_stream = TweetListener()
    twitter_stream.start_streaming_tweets(search_term, collection_name)
