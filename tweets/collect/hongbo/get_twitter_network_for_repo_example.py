import json
import progressbar
import multiprocessing
import pymongo
import math
import sys
sys.path.append("..")
from tw_crawler import Twitter_Crawler
from utils_password import MONGO_PASSWORD, TWITTER_APIKEY_20_TUPLE


tw_id_str_list = list(tw_id_str_set) # input tw_id_str_list

API_key_secret_list_access_token = [TWITTER_APIKEY_20_TUPLE[key_index] for key_index in range(10)]
unidentified_user_set = tw_crawler.get_tw_user_tweets_to_mongo(tw_id_str_list,
                                      API_key_secret_list_access_token,
                                      insert_collection_name = insert_collection_name,
                                      verbose = True,
                                      test_mode = False)
