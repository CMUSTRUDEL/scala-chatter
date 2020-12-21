import collect
import db

def dist(targets, lo, hi, nbucket):
    """
    distribute targets[lo, hi) into nbucket even partitions
    the distribution is used by nbucket processes for parallel computation
    """
    distribution = []
    for _ in range(nbucket):
        distribution.append([])
    for i in range(lo, hi):
        if 0 <= i and i < len(targets):
            distribution[i % nbucket].append(targets[i])
    return distribution

def runner(args):
    """
    run tweets collection on a list of users using one set of apikey, (apikey, users) as args
    the list of users is run sequentially
    establish a new database connection for each user, and commit insertions and close connection when done
    """
    apikey, users = args
    api = collect.mk_api(apikey)
    for user in users:
        db_connection = db.mk_connection()
        collect.collect_user_tweets(api, user, collect.mk_sql_insert_handler(db_connection))
        db.close_connection(db_connection)

if __name__ == '__main__':
    import csv
    import multiprocessing
    import sys

    apikeys = [tuple(s.strip() for s in l.split(',')) for l in open('../../../.tw-apikeys')]
    targets = [r[0] for r in ([l for l in csv.reader(open('../../connections/connection_as_follower_min5.csv'))][1:])]

    # if one command line argument is supplied, treat it as a list of users
    if len(sys.argv) == 2:
        api = collect.mk_api(apikeys[0])
        users = [int(s) for s in sys.argv[1].split(',')]
        for user in users:
            print(user)
            db_connection = db.mk_connection()
            collect.collect_user_tweets(api, user, collect.mk_sql_insert_handler(db_connection), err = 'raise')
            db.close_connection(db_connection)

    # if two command line arguments are supplied, treat them as [lo, hi) indexing into set targets
    if len(sys.argv) == 3:
        lo, hi = int(sys.argv[1]), int(sys.argv[2])
        distribution = dist(targets, lo, hi, len(apikeys))
        try:
            with multiprocessing.Pool(processes = len(apikeys)) as pool:
                [0 for _ in pool.imap_unordered(runner, list(zip(apikeys, distribution)))]
        except KeyboardInterrupt:
            error('KeyboardInterrupt')
            pool.terminate()

