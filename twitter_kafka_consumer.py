from kafka.consumer import KafkaConsumer
from kafka.producer import SimpleProducer
from kafka import KafkaClient

from collections import Counter
import numpy as np
import re
import time
import json


def readSentimentList(file_name):
    ifile = open(file_name, 'r')
    happy_log_probs = {}
    sad_log_probs = {}

    ifile.readline()  # ignore title row

    for line in ifile:
        tokens = line[:-1].split(',')
        happy_log_probs[tokens[0]] = float(tokens[1])
        sad_log_probs[tokens[0]] = float(tokens[2])

    return happy_log_probs, sad_log_probs


def classifySentiment(txt, happy_log_probs, sad_log_probs):
    words = txt.lower().split()
    happy_probs = []
    sad_probs = []
    hashtags = []

    # Get the log-probability of each word under each sentiment
    for word in words:
        if len(word) > 1 and word[0] == '#':
            hashtags.append(re.sub(r'[^\w=]', '', word))

        word = re.sub(r'[^\w=]', '', word)
        if word in happy_log_probs:
            happy_probs.append(happy_log_probs[word])
            sad_probs.append(sad_log_probs[word])

    # Sum all the log-probabilities for each sentiment to get
    # a log-probability for the whole tweet
    tweet_happy_log_prob = np.sum(happy_probs)
    tweet_sad_log_prob = np.sum(sad_probs)

    # Calculate the probability of the tweet belonging to each sentiment
    prob_happy = np.reciprocal(
        np.exp(tweet_sad_log_prob - tweet_happy_log_prob) + 1)
    # prob_sad = 1 - prob_happy
    sentiment = '1' if prob_happy > 0.5 else '-1'
    return hashtags, sentiment


def main():
    happy_log_probs, sad_log_probs = readSentimentList(
        'twitter_sentiment_list.csv')

    consumer = KafkaConsumer("tweets", bootstrap_servers=["localhost:9092"],
                             auto_offset_reset='smallest')

    kafka = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka)
    topic = 'hashtag_sentiment'

    positive_tags = Counter()
    negative_tags = Counter()

    while True:
        for message in consumer.fetch_messages():
            txt = message.value
            txt = re.sub(r'[^\x00-\x7F]', ' ', txt)

            hashtags, sentiment = classifySentiment(
                txt, happy_log_probs, sad_log_probs)

            for hashtag in hashtags:
                if sentiment > 0:
                    positive_tags[hashtag] += 1
                else:
                    negative_tags[hashtag] += 1

        results = {}
        for key, val in positive_tags.most_common(20):
            results[key] = val

        producer.send_messages(topic, json.dumps(results))
        time.sleep(10)


if __name__ == "__main__":
    main()
