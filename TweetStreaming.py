from __future__ import print_function
import tweepy
import json
from pymongo import MongoClient
import datetime

# MONGO_HOST= 'mongodb://localhost/twitterdb'
MONGO_HOST = 'mongodb://dbTbd:dbTbdPassword@clustertbd-shard-00-00-zmoed.mongodb.net:27017,clustertbd-shard-00-01-zmoed.mongodb.net:27017,clustertbd-shard-00-02-zmoed.mongodb.net:27017/test?ssl=true&replicaSet=ClusterTBD-shard-0&authSource=admin&retryWrites=true&w=majority'  # assuming you have mongoDB installed locally
                                             # and a database called 'twitterdb'

WORDS = ['di']
language = ['in']
                                             
consumer_key = "H0RMQsEtbubtEduF5B86MVNxa"
consumer_secret = "rFTi0CaisPiEvSPKUO4U31y4RZqZhdLsP2N9hIfLy69HIIacOC"
access_token = "1139915014305808385-FUKXFmAOfjbt4ORzTDetGtbdRtS0B8"
access_token_secret = "PTaLmlC93BBChhwgAJjy0aJpHB9eSSdMukDQm2zgGlmAb"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

class StreamListener(tweepy.StreamListener):    
    #This is a class provided by tweepy to access the Twitter Streaming API. 

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")
 
    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured : ' + repr(status_code))
        return False
 
    def on_data(self, data):
        #This is the meat of the script...it connects to your mongoDB and stores the tweet
        try:
            client = MongoClient(MONGO_HOST)
            
            # Use twitterdb database. If it doesn't exist, it will be created.
            db = client.twitterdb
    
            # Decode the JSON from Twitter
            datajson = json.loads(data)
            
            # Pull important data from the tweet to store in the database.
            tweet_id = datajson['id_str']  # The Tweet ID from Twitter in string format
            username = datajson['user']['screen_name']  # The username of the Tweet author
            text = datajson['text']  # The entire body of the Tweet
            hashtags = datajson['entities']['hashtags']  # Any hashtags used in the Tweet
            location = datajson['place']
            dt = datajson['created_at']  # The timestamp of when the Tweet was created
            
            # Convert the timestamp string given by Twitter to a date object called "created". This is more easily manipulated in MongoDB.
            created = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')

            # Load all of the extracted Tweet data into the variable "tweet" that will be stored into the database
            tweet = {'id':tweet_id, 'username':username, 'text':text, 'hashtags':hashtags, 'created':created, 'location':location}

            #print out a message to the screen that we have collected a tweet
            print("Tweet collected at " + str(created))
            
            #insert the data into the mongoDB into a collection called twitter_search
            #if twitter_search doesn't exist, it will be created.
            db.twitter_streaming.insert(tweet)
            
        except Exception as e:
           print(e)

#Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True)) 
streamer = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(WORDS))
streamer.filter(languages=language, track=WORDS)