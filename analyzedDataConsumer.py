from kafka import KafkaConsumer
import json
import pymongo
from pymongo import MongoClient
from dotenv import dotenv_values

# from ApiKeys import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET

config = dotenv_values(".env")
print(f"Read the mongo connection details from .env file")

# Establish connection with MongoDB
client = MongoClient(config["ATLAS_URI"])
db = client[config["DB_NAME"]]
collection = db[config["ANALYZED_TWEET_COLLECTION"]]

# Set up Kafka consumer
consumer = KafkaConsumer('analyzed_tweets', bootstrap_servers=['localhost:9092'])

# Iterate over messages and insert into MongoDB
for message in consumer:
    value = message.value.decode('utf-8')
    data = json.loads(value)
    # Insert data into MongoDB
    # collection.insert_one(data)
    collection.replace_one({'tweet': data['tweet']}, data, upsert=True)


