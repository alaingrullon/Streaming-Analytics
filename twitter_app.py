import socket
import sys
import requests
import requests_oauthlib
import json

ACCESS_TOKEN = '1002190093476614144-UKiIT8ItxW5SlOaOMFXSIORRs2UU86'
ACCESS_SECRET = '52sHTHcrY7xGyGdkFBSgU3wHlmrbZkQnYEjognLWRvTbq'
CONSUMER_KEY = 'hzFNAzjV9GUi8r3ORdP0n9ajP'
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

def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            tcp_connection.send(tweet_text + '\n')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

TCP_IP = "localhost"
TCP_PORT = 9007
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)

