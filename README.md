# Real-time Twitter Sentiment

This code provides real time sentiment analysis of tweets. The kafka produces connects to twitter public api, and publishes text to kafka. The consumer reads tweets, perform sentiment analysis, and stores top 20 hashtags with positive sentiment to kafka.

The sentiment of tweets is determined using the method developed by [Alex Davies](http://alexdavies.net/twitter-sentiment-analysis/).
