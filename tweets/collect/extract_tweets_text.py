# copy tweets from connection_tweets table into tweets table
# in the process de-duplicate on (user_id, tweet_id)
# in the process extract only the text of tweets, no meta data

import db

if __name__ == '__main__':
    connection = db.mk_connection()
    users = [t[0] for t in db.execute(connection, 'select distinct user_id from connection_tweets')]
    tweets = {}
    for user in users:
        tweets[user] = [t[0] for t in db.execute(connection, 'select distinct tweet_id from connection_tweets where user_id = %s' % (user))]

    def runner(user):
        try:
            conn = db.mk_connection()
            for tweet in tweets[user]:
                text = db.get_tweet_text(conn, user, tweet).encode('ascii', 'ignore').decode('ascii')
                db.insert_tweet_text(conn, user, tweet, text)
            conn.commit()
            conn.close()
            print('Done', user)
        except:
            print('Exception on', user)

    import multiprocessing
    with multiprocessing.Pool(processes = 40) as pool:
        [0 for _ in pool.imap_unordered(runner, users)]
