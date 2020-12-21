import db
import json

tweets = db.tweets()
print(len(tweets), 'tweets in database')

ts = [(t[0], json.loads(t[2])) for t in tweets]

def objs():
    """ return all tweet objects in database """
    return [t[1] for t in ts]

def infos():
    """ return all tweets as (seed user id, tweet id, text, likes, retweets) tuples """
    return [(
        o['user']['id'],
        o['id'],
        o['text'],
        o['favorite_count'],
        o['retweet_count']
        ) for o in objs()]

def by_likes(limit = -1):
    """ return tweets info sorted by likes count up to limit """
    key_likes = lambda t: t[3]
    res = sorted(infos(), key = key_likes, reverse = True)
    return res[:limit] if limit >= 0 else res

def by_retweets(limit = -1):
    """ return tweets info sorted by retweets count up to limit """
    key_retweets = lambda t: t[4]
    res = sorted(infos(), key = key_retweets, reverse = True)
    return res[:limit] if limit >= 0 else res

def write(obj, f):
    import csv
    with open(f, 'w', newline = '') as csvf:
        writer = csv.writer(csvf)
        writer.writerow(('user_id', 'tweet_id', 'text', 'like_count', 'retweet_count'))
        writer.writerows(obj)

