# find entities -> hashtags in tweets metadata from connection_tweets table
# copy hashtag instances into tweets_hashtags table
# upon inspecting resulted table entries, delete following instances:
# - hashtag like "" (empty string [due to non-ascii chars])
# - hashtag like "0"
# - hashtag like "\_" (single underscore)
# - hashtag like "\_\_" (double underscore)
# - hashtag like "\_\_\_" (triple underscore) ... (8 underscores)

if __name__ == '__main__':
    import db
    import json
    import multiprocessing
    import sys

    connection = db.mk_connection()
    users = [t[0] for t in db.execute(connection, 'select distinct user_id from tweets')]
    user_tweets = {}
    for user in users:
        user_tweets[user] = [t[0] for t in db.execute(connection, 'select distinct tweet_id from tweets where user_id = %s' % user)]
    connection.close()

    def runner(user, sos = False):
        conn = db.mk_connection()
        tweets = user_tweets[user]

        try:
            metadata = {}
            for tweet in tweets:
                metadata[tweet] = db.execute(conn, 'select tweet from connection_tweets where user_id = %s and tweet_id = %s' % (user, tweet))[0][0]
        except:
            print(user, 'Error', '(fetch metadata)')
            if sos:
                raise

        try:
            hashtags = []
            for tweet in tweets:
                tweet_hashtags = json.loads(metadata[tweet])['entities']['hashtags']
                if len(tweet_hashtags) > 0:
                    for hashtag in tweet_hashtags:
                        hashtags.append((tweet, hashtag['text']))
        except:
            print(user, 'Error', '(parse metadata)')
            if sos:
                raise

        try:
            for tweet, hashtag in hashtags:
                db.execute(conn, 'insert into tweets_hashtags values (%s, %s, %s)', has_res = False, args = [user, tweet, hashtag.encode('ascii', 'ignore').decode('ascii')])
            conn.commit()
            conn.close()
        except:
            print(user, 'Error', '(insert hashtag)')
            if sos:
                raise
        print('Done', user)

    if len(sys.argv) >= 2:
        runner(int(sys.argv[1]), sos = True)
    else:
        with multiprocessing.Pool(processes = 12) as pool:
            [0 for _ in pool.imap_unordered(runner, users)]

