import configparser
import json
import logging
from datetime import datetime, timedelta

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


class TwitterListener(StreamListener):
    """A class to read twitter stream and write to json"""

    def __init__(self, config):
        super().__init__()
        self.end_time = datetime.now() + timedelta(hours=config.getint('DEFAULT','duration'))
        self.f=open("resources/tweets.json", "w")

        logging.basicConfig(level=logging.INFO)

        logging.info(f"Streaming will run till... {self.end_time}")

    def on_data(self, data):
        """
        This function is called in case of successful response for listener.
        The stream will run for period defined in config.
        Responses are written to json file.
        :param data:
        :return:
        """
        if datetime.now() < self.end_time:
            # since we are interested in tweets of a particular location
            if json.loads(data).get("place") is not None:
                self.f.write(data)
            return True
        else:
            self.f.close()
            return False

    def on_error(self, status):
        """
        This function is called in case of error in listener.
        :param status:
        :return:
        """
        logging.error(f"Streaming failed with error code {status}")
        return True


if __name__ == '__main__':
    # This handles Twitter authentication and the connection to Twitter Streaming API

    # parse config
    config = configparser.ConfigParser()
    config.read("resources/config")

    # get api keys
    consumer_key =config.get('TWITTER','consumer_key')
    consumer_secret=config.get('TWITTER','consumer_secret')
    access_token=config.get('TWITTER','access_token')
    access_token_secret=config.get('TWITTER','access_token_secret')

    # initialize listerner
    listener = TwitterListener(config)

    # authorize listener
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    options={"retry_count": 10}

    stream = Stream(auth, listener, **options)

    logging.info("Starting sampling tweets...")
    logging.info("Output will be written to resources/tweets.json")
    # start streaming random tweets
    stream.sample()
