REQUEST_TIMEOUT = 3
PROCESSES = 12

if __name__ == '__main__':
    import requests
    import db
    import sys
    import multiprocessing
    import random

    def runner(args):
        user_id, tweet_id, url = args
        try:
            r = requests.get(url, timeout = REQUEST_TIMEOUT)
            try:
                conn = db.mk_connection()
                db.execute(conn, 'update tweets_url set expanded_url = %s where user_id = %s and tweet_id = %s', has_res = False, args = [r.url, user_id, tweet_id])
                db.close_connection(conn)
            except:
                print('Insert Error', user_id, tweet_id, r.url)
        except requests.exceptions.Timeout:
            print('Request Timeout', user_id, tweet_id, url)
        except:
            print('Request Error', user_id, tweet_id, url)
            try:
                conn = db.mk_connection()
                db.execute(conn, 'update tweets_url set expanded_url = %s where user_id = %s and tweet_id = %s', has_res = False, args = ['Request Error', user_id, tweet_id])
                db.close_connection(conn)
            except:
                print('Request Error [Insert Error]', user_id, tweet_id)

    if len(sys.argv) == 3:
        user_id = int(sys.argv[1])
        tweet_id = int(sys.argv[2])
        runner((user_id, tweet_id))

    if len(sys.argv) <= 1:
        table = db.execute(db.mk_connection(), 'select user_id, tweet_id, url, expanded_url from tweets_url')
        print(len(table), 'Table Entries')
        table = [(user_id, tweet_id, url) for user_id, tweet_id, url, expanded_url in table if expanded_url is None]
        print(len(table), 'Not Unshortened Entries')

        random.shuffle(table)
        with multiprocessing.Pool(processes = PROCESSES) as pool:
            [0 for _ in pool.imap_unordered(runner, table)]

