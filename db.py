import mysql.connector

PW_FILE = '../.pw'
DB_USER = 'samyong'
DB_NAME = 'scala_chatter'
USER_TABLE = 'seed_user_details'
FOLLOWER_TABLE = 'seed_user_followers'
FOLLOWING_TABLE = 'seed_user_friends'
TWEET_TABLE = 'seed_user_tweets'
CONNECTION_USER_TABLE = 'seed_connection_details'

pw = [s for s in open(PW_FILE)][0].strip()

db = mysql.connector.connect(
        host='localhost',
        user=DB_USER,
        password=pw,
        database=DB_NAME,
        )

def execute(query, has_res = True):
    cursor = db.cursor()
    cursor.execute(query)
    if not has_res:
        return
    res = cursor.fetchall()
    return res

def seed_users(details=False):
    """
    If details = True, return everything from seed user table
    If details = False, return list of seed user (username, id) pairs
    """
    cursor = db.cursor()
    cursor.execute('select * from ' + USER_TABLE)
    res = cursor.fetchall()
    return res if details else [s[:2] for s in res]

def connection_users():
    """ return list of (connection user id, connection user details json string) """
    cursor = db.cursor()
    cursor.execute('select * from ' + CONNECTION_USER_TABLE)
    res = cursor.fetchall()
    return res

def followers():
    """ return list of (seed user id, follower id) pairs """
    cursor = db.cursor()
    cursor.execute('select * from ' + FOLLOWER_TABLE)
    res = cursor.fetchall()
    return res

def connection_as_follower_count():
    """ return list of (connection user id, # seed users with this connection as follower) """
    cursor = db.cursor()
    cursor.execute('select follower_id, count(*) as s from ' + FOLLOWER_TABLE + ' group by follower_id order by s desc')
    res = cursor.fetchall()
    return res

def followings():
    """ return list of (seed user id, following id) pairs """
    cursor = db.cursor()
    cursor.execute('select * from ' + FOLLOWING_TABLE)
    res = cursor.fetchall()
    return res

def connection_as_following_count():
    """ return list of (connection user id, # seed users with this connection as following) """
    cursor = db.cursor()
    cursor.execute('select friend_id, count(*) as s from ' + FOLLOWING_TABLE + ' group by friend_id order by s desc')
    res = cursor.fetchall()
    return res

def tweets():
    """ return list of (seed user id, tweet id, tweet object json) tuples """
    cursor = db.cursor()
    cursor.execute('select * from ' + TWEET_TABLE)
    res = cursor.fetchall()
    return res

def mk_seed_user_map():
    users = seed_users(True)
    map = {}
    import json
    for username, id, details in users:
        obj = json.loads(details)
        map[id] = (username, obj['name'], obj['followers_count'], obj['friends_count'])
    return map

def mk_connection_user_map():
    users = connection_users()
    map = {}
    import json
    for id, details in users:
        obj = json.loads(details)
        map[id] = (obj['screen_name'], obj['name'], obj['followers_count'], obj['friends_count'])
    return map

