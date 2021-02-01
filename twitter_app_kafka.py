import socket
import sys
import requests
import requests_oauthlib
import json
from kafka import KafkaProducer

ACCESS_TOKEN = '1131112350591930368-3afz8dSfi6xKQ1z0oAt467KWxn91Ta' 
ACCESS_SECRET = 'QeFaYON4aRdybYB2iEu01xvYPRY3hydEb7VXZMR3g0tXKb8Pi4' 
CONSUMER_KEY = '41nmJWp1Pa8ByLsJqAH7uccz3'
CONSUMER_SECRET = 'rOMEcVW5zhcOA0yd8sQ3k3QhVnmrqE9GCkMl324lXew3ucmXGU'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN,
ACCESS_SECRET)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'es'), ('locations', '-3.7834,40.3735,-3.6233,40.4702'),('track','navidad')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response

def send_tweets_to_spark(http_resp, producer, topic):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            producer.send(topic, str(tweet_text))
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

topic = "Alain_Grullon"

producer = KafkaProducer(bootstrap_servers= "data3:6667")

print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, producer, topic)