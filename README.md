# Twitter Sentiment Analysis

As a use case for Natural Language Processing (NLP), this sentiment analysis aims to understand what people think about a topic by analyzing the tweets shared by people over a period of time. Tweepy library is used in this project for accessing the Twitter API. Requested data is streamed through AWS Kinesis and stored in the storage bucket S3. 

NLP and sentiment analysis is conducted using PySpark on Databricks, and the final dashboard is built by AWS Athena and QuickSight as displyed below.

<img width="896" alt="dashboard" src="https://user-images.githubusercontent.com/22150244/207928136-a992f238-ad39-4ba5-bef5-54380aa95d92.png">
