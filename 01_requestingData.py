import tweepy
from tweepy import StreamRule
from tweepy import StreamingClient
# tweepy: tweeter API; Stream: to get live data
import json
# tweets are stored in json format
import boto3 # system package to work with 
import time


class TweetStreamListener(StreamingClient):
    # on success
    def on_data(self, data):
        tweet = json.loads(data)
        try:
            if 'text' in tweet.keys():
                message_lst = [str(tweet['id']),
                       str(tweet['user']['name']),
                       str(tweet['user']['screen_name']),
                       tweet['text'].replace('\n',' ').replace('\r',' '),
                       str(tweet['user']['followers_count']),
                       str(tweet['user']['location']), # locations are not geolocations, users can define locations
                       str(tweet['geo']), # available when users allow twitter to track geo location
                       str(tweet['created_at']), # timestamp of the tweet
                       '\n'
                       ]
                message = '\t'.join(message_lst) # join tweets with tab, to generate a tab delimited file
                print(message) # each message is a list of strings with the above scraped information
                firehose_client.put_record(
                    DeliveryStreamName=delivery_stream_name,
                    Record={
                        'Data': message
                    }
                )
# firehose creates file in Kinesis, stores the scraped data and send it out once the file reachs time/size limit
        except (AttributeError, Exception) as e:
# if the tweet doesn't have any text in it, eg. retweet without any comments, or retweet with a picture etc.,
# return e as error
            print(e)
        return True

    def on_error(self, status):
        print (status)

# configuration
if __name__ == '__main__':
    # create kinesis client connection
    session = boto3.Session() # create a session
    firehose_client = session.client('firehose', region_name='ca-central-1') # ! need to change for each new project

    # Set kinesis data stream name
    delivery_stream_name = 'twitter' # ! need to change for each new project

    # Set twitter credentials
    consumer_key = '******'
    consumer_secret = '******'
    access_token = '******'
    access_token_secret = '******'
    bearer_token = '******'

    while True:
        try:
            print('Twitter streaming...')

            # create instance of the tweet stream listener
            listener = TweetStreamListener(bearer_token)

            # search twitter for the keyword
            # tweet replies without keyword will also be included if the original tweet they replied has the key word
            listener.add_rules(StreamRule('climate change'))
            listener.filter()
            
        except Exception as e:
            print(e)
            print('Disconnected...')
            time.sleep(900)
            continue
