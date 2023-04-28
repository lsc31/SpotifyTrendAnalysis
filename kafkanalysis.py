from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
from textblob import TextBlob
import os
from pyspark.sql.functions import *
import re
# from pymongo import MongoClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pickle

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 pyspark-shell'

# Using VADER for sentiment analysis
analyzer = SentimentIntensityAnalyzer()

def sentiment_analysis(tweet):
    vs = analyzer.polarity_scores(tweet)
    return vs['compound']

def getSentiment(polarityValue: float) -> str:
    if polarityValue < -0.05:
        return 'Negative'
    elif polarityValue > 0.05:
        return 'Positive'
    else:
        return 'Neutral'

# Using textblob for analysis
def textblob_sentiment_analysis(tweet):
    blob = TextBlob(tweet)
    return blob.sentiment.polarity

def getTextblobSentiment(polarityValue: int) -> str:
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
sentiment_analysis_vader_udf = udf(sentiment_analysis, FloatType())
df = df.withColumn("vader_sentiment_score", sentiment_analysis_vader_udf("processed_text"))
sentiment_label_udf = udf(getSentiment, StringType())
df = df.withColumn("vader_sentiment", sentiment_label_udf(col("vader_sentiment_score")))

sentiment_analysis_textblob_udf = udf(textblob_sentiment_analysis, FloatType())
df = df.withColumn("textblob_sentiment_score", sentiment_analysis_textblob_udf("processed_text"))
textblob_sentiment_label_udf = udf(getTextblobSentiment, StringType())
df = df.withColumn("textblob_sentiment", textblob_sentiment_label_udf(col("textblob_sentiment_score")))

# Using SVM for analysis
with open('svm_model.pickle', 'rb') as f:
    svm_model = pickle.load(f)

# Define a UDF to apply the SVM model to each tweet
svm_sentiment_udf = udf(lambda tweet: svm_model.predict([tweet])[0], StringType())

# Apply the SVM model to the tweets in the DataFrame
df = df.withColumn('svm_sentiment', svm_sentiment_udf(col('processed_text')))

mongoURL = "mongodb+srv://<username>:<password>@cluster0.mwzfmxm.mongodb.net/spotifycharts.cleandata" \
               "?retryWrites=true&w=majority"


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


