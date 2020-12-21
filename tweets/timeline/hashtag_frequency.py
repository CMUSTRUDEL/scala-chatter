import db

def normalize(hashtag):
    """ lowercase """
    hashtag = hashtag.lower()
    return hashtag

def map():
    """ create hashtag -> (user_id, tweet_id) list map
    frequency of hashtag = len(hashtag_map[hashtag])
    rank all hashtags = sorted(hashtag_map.keys(), key = lambda hashtag: len(hashtag_map[hashtag]), reverse = True) """
    table = db.execute(db.mk_connection(), 'select user_id, tweet_id, hashtag from tweets_hashtags')
    print(len(table), 'Table Entries')
    hashtag_map = {}
    for user_id, tweet_id, hashtag in table:
        hashtag = normalize(hashtag)
        if hashtag not in hashtag_map:
            hashtag_map[hashtag] = []
        hashtag_map[hashtag].append((user_id, tweet_id))
    return hashtag_map

