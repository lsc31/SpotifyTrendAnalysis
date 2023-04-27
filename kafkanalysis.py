from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
from textblob import TextBlob
import os
from pyspark.sql.functions import *
import re
# from pymongo import MongoClient

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 pyspark-shell'


def sentiment_analysis(tweet):
    blob = TextBlob(tweet)
    return blob.sentiment.polarity

def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'

def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # remove puntuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # remove number
    # tweet = re.sub('([0-9]+)', '', str(tweet))

    # remove hashtag
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    return tweet

def write_row_in_mongo(df):
    mongoURL = "mongodb+srv://<username>:<password>@cluster0.mwzfmxm.mongodb.net/spotifycharts.cleandata" \
               "?retryWrites=true&w=majority"
    df.write.format("mongo").mode("append").option("uri", mongoURL).save()
    pass

conf = SparkConf().setAppName('Kafka-Sentiment-Analysis')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# spark = SparkSession.builder \
#     .appName("YourAppName") \
#     .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
#     .getOrCreate()

schema = StructType([
    StructField("id", StringType()),
    StructField("track", StringType()),
    StructField("tweet", StringType())
])

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "twitter") \
  .load() \
  .select(from_json(col("value").cast("string"), schema).alias("data")) \
  .select("data.*")

preprocessed_udf = udf(cleanTweet,StringType())
df = df.withColumn('processed_text', preprocessed_udf(col("tweet")))
sentiment_analysis_udf = udf(sentiment_analysis, FloatType())
df = df.withColumn("sentiment_score", sentiment_analysis_udf("processed_text"))
sentiment_label_udf = udf(getSentiment, StringType())
df = df.withColumn("sentiment", sentiment_label_udf(col("sentiment_score")))

mongoURL = "mongodb+srv://<username>:<password>@cluster0.mwzfmxm.mongodb.net/spotifycharts.cleandata" \
               "?retryWrites=true&w=majority"

# write_to_mongo = df \
#   .writeStream \
#   .foreachBatch(lambda batch_df, batch_id: batch_df.write.format("mongo").mode("append").option("uri",mongoURL).save())

# res = write_row_in_mongo(df)

def write_mongo(batch_df, batch_id):
    batch_df.write.format("mongo") \
        .mode("append") \
        .option("uri", mongoURL) \
        .save()

# streaming_query = df \
#   .writeStream \
#   .format("console") \
#   .option("truncate", "false") \
#   .foreachBatch(write_mongo) \
#   .start()

streaming_query = df \
  .writeStream \
  .format("console") \
  .option("truncate", "false") \
  .start()

streaming_query.awaitTermination()


