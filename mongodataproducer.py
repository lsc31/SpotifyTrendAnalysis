from kafka import KafkaProducer
import logging
import os
import json
from pymongo import MongoClient

mongodb_client = MongoClient("mongodb+srv://<username>:<password>@cluster0.mwzfmxm.mongodb.net/?retryWrites=true&w=majority")
print("Mongo client created")

def get_database():
    return mongodb_client["spotifycharts"]

def get_collection_name(dbname):
    return dbname["tweets"]

producer = KafkaProducer(bootstrap_servers='localhost:9092')
# search_term = 'Search & Rescue'
search_term = "Last Night"
topic_name = 'twitter'


class TweetListener():

    # def on_data(self, raw_data):
    #     logging.info(raw_data)
    #     producer.send(topic_name, value=raw_data)
    #     return True

    # def on_error(self, status_code):
    #     if status_code == 420:
    #         # returning False in on_data disconnects the stream
    #         return False

    def start_streaming_tweets(self, search_term, collection_name):
        raw_tweets = collection_name.find({"track":search_term})
        if(raw_tweets):
            for t in raw_tweets:
                # logging.info(t)
                print(t)
                producer.send(topic_name, value=json.dumps(t,default=str).encode('utf-8'))
                # producer.send(topic_name, b'Hello')
        else:
            print("No records found")
        

if __name__ == '__main__':
    dbname = get_database()
    collection_name = get_collection_name(dbname)
    twitter_stream = TweetListener()
    twitter_stream.start_streaming_tweets(search_term, collection_name)
    producer.close()
