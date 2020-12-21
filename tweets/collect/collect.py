import db
import time
import tweepy

def mk_api(keys):
    """ create tweepy api instance for a set of apikey """
    consumer_key, consumer_secret, access_token, access_token_secret = keys
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return tweepy.API(auth, wait_on_rate_limit = True, wait_on_rate_limit_notify = True)

def collect_user_tweets(api, user_id, tweet_handler, err = 'silent'):
    """
    collect all tweets from user_id using tweepy api instance
    pass each tweet item over to tweet_handler continuation function
    when collecting tweets to a sql table, perform database insertion in tweet_handler
    """
    def rate_limit_cursor(cursor):
        """ sleep api cursor when rate limit is hit """
        while True:
            try:
                yield cursor.next()
            except tweepy.RateLimitError:
                print('RateLimitError', user_id)
                time.sleep(15 * 60)
    try:
        for status in rate_limit_cursor(tweepy.Cursor(api.user_timeline, id = user_id, tweet_mode = 'extended').items()):
            tweet_handler(status)
    except tweepy.TweepError as e:
        print('API Error', user_id, e.response.text)
        if e.response.text == '{"errors":[{"message":"Rate limit exceeded","code":88}]}':
            time.sleep(3 * 60)
            return
        if err == 'raise':
            raise e

def mk_sql_insert_handler(db_connection):
    """
    use an existing database connection to insert a single tweet item
    note that the insertion does not take effect until the database connection is commited
    thus the caller is responsible for commiting and closing the connection
    """
    def insert_handler(tweet):
        obj = tweet._json
        db.insert_tweet(db_connection, obj['user']['id'], obj['id'], obj)
    return insert_handler

