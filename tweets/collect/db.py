import mysql.connector
import json

PW_FILE = '../../../.pw'
DB_USER = 'samyong'
DB_NAME = 'scala_chatter'
TWEETS_TABLE = 'connection_tweets'
TWEETS_TEXT_TABLE = 'tweets'

pw = [s for s in open(PW_FILE)][0].strip()
def mk_connection():
    return mysql.connector.connect(host = 'localhost', user = DB_USER, password = pw, database = DB_NAME)

def insert_tweet(connection, user_id, tweet_id, tweet):
    cursor = connection.cursor()
    cursor.execute('INSERT INTO `{}` VALUES (%s, %s, %s)'.format(TWEETS_TABLE), (user_id, tweet_id, json.dumps(tweet)))

def get_tweet_text(connection, user_id, tweet_id):
    cursor = connection.cursor()
    cursor.execute('SELECT DISTINCT tweet FROM %s WHERE user_id = %s AND tweet_id = %s' % (TWEETS_TABLE, user_id, tweet_id))
    obj = json.loads(cursor.fetchall()[0][0])
    return obj['full_text']

def insert_tweet_text(connection, user_id, tweet_id, text):
    cursor = connection.cursor()
    cursor.execute('INSERT INTO `{}` VALUES (%s, %s, %s)'.format(TWEETS_TEXT_TABLE), (user_id, tweet_id, text))

def close_connection(connection):
    connection.commit()
    connection.close()

def execute(connection, query, has_res = True, args = None):
    cursor = connection.cursor()
    if args is None:
        cursor.execute(query)
    else:
        cursor.execute(query, args)
    if has_res:
        res = cursor.fetchall()
        return res
