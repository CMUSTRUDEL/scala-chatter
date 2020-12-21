import db

def normalize(url):
    """ lowercase url
    remove 'http://' and 'https://' prefix
    remove '/' suffix """
    url = url.lower()
    if url[:7] == 'http://':
        url = url[7:]
    if url[:8] == 'https://':
        url = url[8:]
    if url[-1] == '/':
        url = url[:-1]
    return url

def map():
    """ create url -> (user_id, tweet_id) list map
    frequency of url = len(url_map[url])
    rank all urls = sorted(url_map.keys(), key = lambda url: len(url_map[url]), reverse = True) """
    table = db.execute(db.mk_connection(), 'select user_id, tweet_id, url, expanded_url from tweets_url')
    print(len(table), 'Table Entries')
    url_map = {}
    for user_id, tweet_id, url, expanded_url in table:
        url = normalize(url) if expanded_url is None or expanded_url == 'Request Error' else normalize(expanded_url)
        if url not in url_map:
            url_map[url] = []
        url_map[url].append((user_id, tweet_id))
    return url_map

