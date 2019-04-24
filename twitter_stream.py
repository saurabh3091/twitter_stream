import configparser
import json
import logging
from datetime import datetime, timedelta

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


class TwitterListener(StreamListener):
    def __init__(self):
        super().__init__()
        self.end_time = datetime.now() + timedelta(hours=72)
        self.f=open("resources/tweets.json", "w")

        logging.info(f"Starting sampling twitter... \nStreaming will end at {self.end_time}")

    def on_data(self, data):
        if datetime.now() < self.end_time:
            # since we are interested in tweets of a particular location
            if json.loads(data).get("place") is not None:
                print(data)
                self.f.write(data)
            return True
        else:
            self.f.close()
            return False

    def on_error(self, status):
        logging.error(f"Streaming failed with error code {status}")
        return True


if __name__ == '__main__':

    #This handles Twitter authentication and the connection to Twitter Streaming API
    config = configparser.ConfigParser()
    config.read("resources/config")

    consumer_key =config['DEFAULT']['consumer_key']
    consumer_secret=config['DEFAULT']['consumer_secret']
    access_token=config['DEFAULT']['access_token']
    access_token_secret=config['DEFAULT']['access_token_secret']

    listener = TwitterListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    options={"retry_count": 10}
    stream = Stream(auth, listener, **options)

    # we need tweets from a large sample without filters
    stream.sample()
