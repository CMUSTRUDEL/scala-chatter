if __name__ == '__main__':
    import db
    import json
    import datetime
    import multiprocessing
    import sys

    connection = db.mk_connection()
    users = [t[0] for t in db.execute(connection, 'select distinct user_id from tweets')]
    user_tweets = {}
    for user in users:
        user_tweets[user] = [t[0] for t in db.execute(connection, 'select distinct tweet_id from tweets where user_id = %s' % user)]
    connection.close()

    def runner(user, sos = False):
        try:
            tweets = user_tweets[user]
            conn = db.mk_connection()
            for ts in tweets:
                try:
                    tweet = db.execute(conn, 'select tweet from connection_tweets where user_id = %s and tweet_id = %s' % (user, ts))[0][0]
                    obj = json.loads(tweet)
                    t = obj['created_at']
                    t = datetime.datetime.strptime(t, '%a %b %d %H:%M:%S +0000 %Y').strftime('%Y-%m-%d %H:%M:%S')
                    db.execute(conn, 'update tweets set created_at = %s where user_id = %s and tweet_id = %s', has_res = False, args = [t, user, ts])
                except:
                    print('Error', user, ts, t)
                    if sos:
                        raise
            conn.commit()
            conn.close()
            print('Done', user)
        except:
            if sos:
                raise
            print('Error', user)

    if len(sys.argv) >= 2:
        run_users = [int(s) for s in sys.argv[1].split(',')]
        sos = True if len(sys.argv) == 3 and sys.argv[2] == '--sos' else False
        [runner(user, sos) for user in run_users]
    else:
        with multiprocessing.Pool(processes = 12) as pool:
            [0 for _ in pool.imap_unordered(runner, users)]
