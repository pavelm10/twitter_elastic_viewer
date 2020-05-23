import logging
import json
import datetime
import tweepy
from logstash import UDPLogstashHandler

from elasticsearch_reader import ElasticReader
from helpers import simple_logger


class TwitterCrawler(tweepy.streaming.StreamListener):

    def __init__(self, auth_path_json, es_index, es_search_date, es_port=9200, logstash_port=41000, max_tweets=3200):
        super().__init__()

        with open(auth_path_json, 'r') as f:
            self.__auth_json = json.loads(f.read())

        self.__auth = tweepy.OAuthHandler(self.__auth_json['api_key'], self.__auth_json['api_key_secret'])
        self.__auth.set_access_token(self.__auth_json['access_token'], self.__auth_json['access_token_secret'])
        self.api = tweepy.API(self.__auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        self.logstash_port = logstash_port
        self.max_tweets = max_tweets
        self.processed_tweets = 0
        self.search_start = es_search_date

        self.log = simple_logger()
        self.stash = self.setup_stash()
        self.es_reader = ElasticReader(es_index=es_index, es_port=es_port)
        self.stream = tweepy.Stream(auth=self.api.auth, listener=self)

        self.all_ids = self.es_reader.read_all_ids(self.search_start)
        self.following_uids = self.get_following()

    def on_error(self, status_code):
        if status_code == 420:
            self.log.warning('status code 420, too many requests, disconnecting streaming...')
            return False

    def on_status(self, status):
        self.process_tweet(status)

    def get_following(self):
        self.log.info('Getting list of the following users')
        following_ids = {friend.id_str for friend in tweepy.Cursor(self.api.friends).items()}
        return following_ids

    def setup_stash(self):
        stash = logging.getLogger('stash')
        stash.setLevel(logging.INFO)
        stash.addHandler(UDPLogstashHandler('localhost', self.logstash_port))
        return stash

    def build_history(self):
        max_id = 0
        if len(self.all_ids) > 0:
            max_id = min(self.all_ids)

        self.log.info(f'Building history beyond tweet_id: {max_id}')
        while max_id > 0 and self.processed_tweets < self.max_tweets:
            max_id = self.process_my_timeline(max_id=max_id)

    def update_latest(self):
        if len(self.all_ids) > 0:
            upto_id = max(self.all_ids)
        else:
            self.log.info('No data in the database...')
            return

        self.log.info('Updating latest tweets')
        max_id = self.process_my_timeline()
        while max_id > upto_id and self.processed_tweets < self.max_tweets:
            max_id = self.process_my_timeline(max_id=max_id)

    def streaming(self):
        self.log.info('Starting streaming')
        if self.following_uids:
            self.stream.filter(follow=self.following_uids)
            self.log.info('Streaming finished')
        else:
            self.log.error('The list of friends is empty')

    def process_my_timeline(self, count=200, max_id=0):
        if max_id > 0:
            self.log.info(f'Getting tweets till ID: {max_id}')
            tweets = list(tweepy.Cursor(self.api.home_timeline, max_id=max_id).items(count))
        else:
            self.log.info(f'Getting last {count} tweets')
            tweets = list(tweepy.Cursor(self.api.home_timeline).items(count))

        for tweet in tweets:
            self.process_tweet(tweet)

        if len(tweets) > 0:
            oldest = tweets[-1].id - 1
        else:
            oldest = 0
            self.log.info('Obtained empty iterator')

        return oldest

    def process_tweet(self, tweet):
        if self.validate_tweet(tweet):
            url = tweet.entities['urls'][0]['expanded_url'] if len(tweet.entities['urls']) else ''
            date = self.convert2datetime(tweet._json['created_at'])
            out = {'tweet_id': tweet.id,
                   'tweet_text': tweet._json['text'],
                   'tweet_url': url,
                   'tweet_user_id': tweet.user.id,
                   'tweet_user_screen_name': tweet.user.screen_name,
                   'tweet_time_creation': tweet._json['created_at'],
                   'tweet_year': int(date.year),
                   'tweet_month': int(date.month),
                   'tweet_day': int(date.day),
                   'tweet_hour': int(date.hour),
                   'tweet_minute': int(date.minute),
                   'tweet_second': int(date.second),
                   'tweet_location': tweet._json.get('location')}

            self.log.info(f'Stashing: {out}')
            self.stash.info(msg=out['tweet_text'], extra=out)

            self.all_ids.add(tweet.id)
            self.processed_tweets += 1

    def validate_tweet(self, tweet):
        valid = True
        valid &= tweet.id not in self.all_ids
        valid &= tweet.user.id_str in self.following_uids
        return valid

    def convert2datetime(self, time_string):
        splitted = time_string.split(" ")
        splitted.remove("+0000")
        time_string = " ".join(splitted)
        date = datetime.datetime.strptime(time_string, "%a %b %d %H:%M:%S %Y")
        return date


if __name__ == "__main__":
    import argparse

    argp = argparse.ArgumentParser()
    argp.add_argument('--auth-json',
                      dest='auth_json',
                      required=True)
    argp.add_argument('--logstash-port',
                      dest='logstash_port',
                      default=41000)
    argp.add_argument('--es-port',
                      dest='es_port',
                      default=9200)
    argp.add_argument('--start-date',
                      dest='start_date',
                      required=True)
    argp.add_argument('--index',
                      default='tweets')
    argp.add_argument('--max-tweets',
                      dest='max_tweets',
                      default=3200)
    argp.add_argument('-s',
                      dest='stream',
                      action='store_true')

    args = argp.parse_args()

    crawler = TwitterCrawler(args.auth_json, args.index, args.start_date, args.es_port, args.logstash_port, args.max_tweets)

    if args.stream:
        crawler.update_latest()
        crawler.streaming()
    else:
        crawler.build_history()
