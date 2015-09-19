import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from kafka import SimpleProducer, KafkaClient


with open('../twitter_credentials.txt') as f:
    ckey, csecret, atoken, asecret = [line.strip() for line in f.readlines()]

kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)
topic = 'tweets'


class listener(StreamListener):

    def on_data(self, data):
        all_data = json.loads(data)
        if 'text' in all_data:
            tweet = all_data["text"]
            # matchObj = re.match( r'(\$|#)[\w.-]{1,5}\s', tweet)
            # if matchObj:
            if '#' in tweet:  # get tweets with hashtags
                producer.send_messages(topic, tweet.encode('utf-8'))
                # print tweet
        return True

    def on_error(self, status):
        print status

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener())
twitterStream.filter(locations=[-180, -90, 180, 90], languages=['en'])
