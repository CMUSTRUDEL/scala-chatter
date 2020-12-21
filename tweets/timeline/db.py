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
