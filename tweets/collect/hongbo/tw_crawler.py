import tweepy
import multiprocessing
import math
import pymongo
import progressbar
import sys
sys.path.append("..")
from utils_password import MONGO_PASSWORD

insert_every = 100


class Twitter_Crawler():

    def _get_user_tweets(self,input):
        client = pymongo.MongoClient(host='localhost', username = 'ght', 
            password = MONGO_PASSWORD, authSource = 'twitter', port=27017)
        db = client.twitter


        auth = tweepy.OAuthHandler(input[0], input[1])
        auth.set_access_token(input[2], input[3])
        api = tweepy.API(auth, wait_on_rate_limit = True)
        data = input[4]
        insert_collection_name = input[5]
        process_sequence = input[6]
        collection = db[insert_collection_name]
        range_ = range(len(data))
        if process_sequence == 0:
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        result_list = []
        unidentified_error_set = set()
        for row_index in range_:

            related_id = str(data[row_index])

            continue_signal = False
            full_tweet_list = []
            try:
                for page in tweepy.Cursor(api.user_timeline, id = related_id, count = 100, tweet_mode = 'extended').pages(): 
                    for status in page:
                        full_tweet_list.append(status._json)
            except tweepy.TweepError:
                unidentified_error_set.add(related_id)
                continue_signal = True
            except Exception as e2:
                print("unexpected error", e2)
                assert False
            if continue_signal == True:
                continue
            if len(full_tweet_list) > 0:
                try: 
                    collection.insert(full_tweet_list)
                except pymongo.errors.AutoReconnect as py_error:
                    for reconnect_attemp_index in range(MAX_AUTO_RECONNECT_ATTEMPTS):
                        try:
                            collection.insert(full_tweet_list)
                            break
                        except pymongo.errors.AutoReconnect as py_error:
                            wait_t = 0.5 * pow(2, reconnect_attemp_index)
                            time.sleep(wait_t)
                except Exception as unknown_error:
                    print("unknown error", unknown_error)
                    print("related_id", related_id)
                    assert False

        if process_sequence == 0:
            p.finish()

        return unidentified_error_set


    def _get_user_profile(self,input):
        # input is a tuple of following information:
        # (key(4 items), data(1 item), process_index (1 item))
        client = pymongo.MongoClient(host='localhost', username = 'ght', 
            password = MONGO_PASSWORD, authSource = 'twitter', port=27017)
        db = client.twitter
        
        auth = tweepy.OAuthHandler(input[0], input[1])
        auth.set_access_token(input[2], input[3])
        api = tweepy.API(auth, wait_on_rate_limit = True)
        data = input[4]
        insert_collection_name = input[5]
        process_sequence = input[6]
        collection = db[insert_collection_name]
        range_ = range(len(data))
        if process_sequence == 0:
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        result_list = []
        unidentified_user_id_int_set = set()
        for row_index in range_:
            if (len(result_list) + 1)% insert_every == 0:
                collection.insert(result_list)
                result_list = [] # empty the list

            tweet_user_id_int = data[row_index]

            continue_signal = False
            try:
                user = api.get_user(user_id = tweet_user_id_int)
            except tweepy.error.TweepError as e1:
                unidentified_user_id_int_set.add(tweet_user_id_int)
                continue_signal = True
            except Exception as e2:
                assert False
            if continue_signal == True:
                continue

            result_list.append(user.__dict__['_json'])

        if len(result_list) > 0:
            collection.insert(result_list)
            result_list = [] # empty the list

        if process_sequence == 0:
            p.finish()

        return unidentified_user_id_int_set


    def __pack_user_id_list(self,
                            user_id_list, 
                            tw_api_list_of_list,
                            insert_collection_name, 
                            multi_thread_function,
                            verbose = True):

        total_len_data = len(user_id_list)
        total_process_count = len(tw_api_list_of_list)
        batch_size = int(math.ceil(total_len_data * 1.0 / total_process_count))
        pool = multiprocessing.Pool(total_process_count) 

        
        if verbose == True:
            print("total_process_count",total_process_count)
            print("total user to be crawled", total_len_data)
        split_data = [[] for _ in range(total_process_count)]
        for data_batch_index in range(total_process_count):
            for data_index in range(batch_size*data_batch_index, batch_size*(data_batch_index + 1)):
                if data_index < total_len_data:
                    split_data[data_batch_index].append(user_id_list[data_index])

        for data_batch_index in range(total_process_count):
            tw_api_list_of_list[data_batch_index].append(split_data[data_batch_index])
            tw_api_list_of_list[data_batch_index].append(insert_collection_name)
            tw_api_list_of_list[data_batch_index].append(data_batch_index)

        results = pool.map_async(multi_thread_function, tw_api_list_of_list).get()

        unidentified_user_set = set()
        for result in results:
            unidentified_user_set |= result

        return unidentified_user_set

    def get_tw_user_profile_to_mongo(self,
                                     user_id_list, 
                                     tw_api_list_of_list,
                                     insert_collection_name, 
                                     verbose = True,
                                     test_mode = False):
        if test_mode == True:
            user_id_list = user_id_list[:20]
        unidentified_user_set = \
            self.__pack_user_id_list(user_id_list,tw_api_list_of_list,insert_collection_name, self._get_user_profile, verbose)

        return unidentified_user_set



    def get_tw_user_tweets_to_mongo(self,
                                    user_id_list, 
                                    tw_api_list_of_list,
                                    insert_collection_name, 
                                    verbose = True, 
                                    test_mode = False): # test only crawls one sample
        if test_mode == True:
            user_id_list = user_id_list[:20]
        unidentified_user_set = \
            self.__pack_user_id_list(user_id_list,tw_api_list_of_list,insert_collection_name, self._get_user_tweets, verbose)

        return unidentified_user_set
