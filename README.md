# SpotifyTrendAnalysis


This is a Big data Analytics project to conduct regional trend analysis on Spotify trending songs.

##Introduction
Spotify is a popular music streaming service that allows users to access millions of songs and podcasts from various genres and languages. In addition to its music streaming capabilities, Spotify also offers various features such as social sharing, collaborative playlists, and customized radio stations. One of the key features of Spotify is its ability to generate real-time music charts based on user listening data. These charts provide insights into the most popular songs and artists on the platform, as well as trends in music consumption. Spotify's charts are a valuable resource for music enthusiasts, industry professionals, and researchers alike, as they can help identify emerging trends, track the success of specific songs or artists, and inform marketing and promotional strategies.

In this work, the data collected from Spotify's charts is analyzed to identify patterns and trends in music consumption, and also gain insights about fans reaction to the trending songs using Sentiment Analysis.

##Initial Analysis
Initial analysis on Spotify charts data answers the following questions:

1.	Get top 10 artists based on the number of their occurrence in top200 over the period 2017-2021
2.	Top 10 songs of all time based on the streams
3.	The number of times the top 10 songs appeared in top200
4.	Rank 1 songs and the number of days in the position
5.	Top 20 Artists who were at Rank 1
6.	Is rank and stream count correlated?

##Sentiment Analysis for Audience reaction to trending music
One way to analyze audience reaction to a trend is using Social media. In this case Twitter API is used as a medium to extract tweets relevant to the trending songs to analyze the popularity of the song.
Sentiment analysis uses Machine learning algorithms to automatically classify the tweets as positive, negative or neutral. Listening to the audience reaction is key to improve the music composition. It also helps to see the aspects of the music that are most liked and disliked to make business decisions.

##To run in local environment:
Install Kafka server on your local system. 
Start the zookeeper service using, "bin/zookeeper-server-start.sh config/zookeeper.properties".
Start the kafka server using, "bin/kafka-server-start.sh config/server.properties"
Start the spark analysis script - "python kafkanalysis.py"
Start the consumer for analyzed_tweets topic to save results to Mongodb - "python analyzedDataConsumer.py"
Start the tweet data producer - "python mongodataproducer.py -h" to show the list of songs in the chart. Issue track title as argument to run the script. For example, "python mongodataproducer.py 'Search & Rescue'".



