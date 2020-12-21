import os
import sys
import pymysql
import pandas as pd
import numpy as np
from prepare_api import load_clients
from prepare_api import shift_clients
import traceback
import json
import time
import datetime
import urllib

def connect():
    connection = pymysql.connect(user='MONGO_USER', password='MONGO_PWD', db='scala_chatter', charset='utf8', use_unicode=True)
    print ("connect successful!!")
    return connection

def get_users(conn, LOG_USERS):
    query = """
    SELECT user_id FROM seed_user_details
    """
    df = pd.read_sql(query, conn)

    user_ids = df['user_id'].values

    users_already_done = set()

    f = open(LOG_USERS, 'r')
    line = f.readline()
    while line:
        line = line.replace('\n', '')
        users_already_done.add(int(line))
        line = f.readline()
    f.close()

    users_to_crawl = set()

    for i in range(len(user_ids)):
        id = int(user_ids[i])

        if id not in users_already_done:
            users_to_crawl.add(id)

    print("LENGTH of USERS TO CRAWL="+str(len(users_to_crawl)))
    return users_to_crawl

def load_api_clients():
    f=open('../config_files/api_accounts.json','r')
    json_arr=json.load(f)
    arr_clients=load_clients(json_arr)
    f.close()
    return arr_clients

def grab_all_tweets(conn, dest_table, account, api_clients, flog, ferr):
    curr_client_id = 0
    curr_client = api_clients[curr_client_id]
    user_tweets_url = 'https://api.twitter.com/1.1/statuses/user_timeline.json?id='

    while (curr_client.get_remaining_hits('statuses', '/statuses/user_timeline') < 2):
        print("Current Client Timing Out" + str(curr_client_id))
        curr_client_id, curr_client = shift_clients(curr_client_id, api_clients)

    cursor = conn.cursor()
    n_status = 1
    resp = 404
    min_id = float('inf')
    try:
        ctr = 0
        while (resp != 200):
            if (ctr >= 5):
                break;
            print("Trying to get timeline for:" + str(account) + " for " + str(ctr) + " th time.")

            while (curr_client.get_remaining_hits('statuses', '/statuses/user_timeline') < 2):
                print("Current Client Timing Out" + str(curr_client_id))
                curr_client_id, curr_client = shift_clients(curr_client_id, api_clients)

            f1, resp = curr_client.get_request(user_tweets_url + str(account) + '&count=200&include_rts=1')
            ctr = ctr + 1

            curr_client_id, curr_client = shift_clients(curr_client_id, api_clients)

            if (ctr >= 5):
                print("Cannot get response for:" + str(account))
                ferr.write("Cannot get response for:" + str(account)+"\n")
                break

        statuses_info = json.loads(f1)
        n_status, min_id = insert_tweets(cursor, dest_table, account, statuses_info, min_id, n_status)
        conn.commit()

        counter = 2
        while (counter <= 17 and n_status >= 1):
            resp = 400
            ctr = 0
            n_status = 0
            while (resp != 200):
                if (ctr >= 5):
                    break;
                print("Trying to get timeline for:" + str(account) + " for " + str(ctr) + " th time.")

                while (curr_client.get_remaining_hits('statuses', '/statuses/user_timeline') < 2):
                    print("Current Client Timing Out" + str(curr_client_id))
                    curr_client_id, curr_client = shift_clients(curr_client_id, api_clients)

                f1, resp = curr_client.get_request(
                    user_tweets_url + str(account) + '&count=200&include_rts=1&max_id=' + str(min_id))
                ctr = ctr + 1

                curr_client_id, curr_client = shift_clients(curr_client_id, api_clients)

                if (ctr >= 5):
                    "Cannot get response for:" + str(account)
                    ferr.write("Cannot get response for:" + str(account) + "\n")
                    break

            statuses_info = json.loads(f1)
            n_status, min_id = insert_tweets(cursor, dest_table, account, statuses_info, min_id, n_status)
            conn.commit()

            print("N_Status=" + str(n_status))
            counter = counter + 1

    except Exception as e:
        print("Exception while doing this." + str(e))
        ferr.write("Cannot get response for:" + str(account) + "\n")
        traceback.print_exc(file=sys.stdout)

def insert_tweets(cursor, dest_table, account, statuses_info, min_id, n_status):
    values = []
    for tweet in statuses_info:
        n_status = n_status + 1
        tweet_id = int(tweet['id_str'])

        values.append([str(account), str(tweet_id), json.dumps(tweet)])

        if (min_id > tweet_id):
            min_id = tweet_id

    query = "INSERT INTO "+str(dest_table)+" (seed_id, tweet_id, tweet) VALUES (%s, %s, %s)"
    cursor.executemany(query, values)

    return n_status, min_id

def crawl_tweets(conn, dest_table, api_clients, users_to_crawl, LOG_USERS, ERR_USERS):
    flog = open(LOG_USERS, 'a')
    ferr = open(ERR_USERS, 'a')

    users_to_crawl = list(users_to_crawl)
    print(users_to_crawl)

    for account in users_to_crawl:
        #account = users_to_crawl[i]
        grab_all_tweets(conn, dest_table, account, api_clients, flog, ferr)

        flog.write(str(account)+'\n')
        flog.flush()
        ferr.flush()

    flog.close()
    ferr.close()

if __name__ == '__main__':
    conn = connect()

    f = open('../config_files/config.json', 'r')
    config = json.load(f)
    f.close()

    dest_table = config['tweets']['table_name']
    log_file = config['tweets']['log_file']
    err_file = config['tweets']['error_file']
    api_clients = load_api_clients()

    users_to_crawl = get_users(conn, log_file)
    print(type(users_to_crawl))
    crawl_tweets(conn, dest_table, api_clients, users_to_crawl, log_file, err_file)