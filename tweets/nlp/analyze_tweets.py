import db

# users = [t[0] for t in db.execute('select distinct user_id from tweets')]

# def get_document(user):
#     """ Combine all tweets from user into single document. """
#     tweets = [t[0] for t in db.execute('select tweet_text from tweets where user_id = %s' % user)]
#     return ' '.join(tweets)

# def extract_topics(users_range = (0, len(users)), n_features = 1000, n_topics = 10, n_top_words = 15):
#     from sklearn.feature_extraction.text import TfidfVectorizer
#     from sklearn.decomposition import NMF
#
#     data = [get_document(user) for user in users[users_range[0] : users_range[1]]]
#     vec = TfidfVectorizer(max_df = 0.95, min_df = 2, max_features = n_features, stop_words = 'english')
#     tfidf = vec.fit_transform(data)
#     nmf = NMF(n_components = n_topics, random_state = 1).fit(tfidf)
#     feature_names = vec.get_feature_names()
#     return ['Topic #%d: ' % topic_idx + '|'.join([feature_names[i] for i in topic.argsort()[:-n_top_words - 1:-1]]) for topic_idx, topic in enumerate(nmf.components_)]

# def nltk_collection():
#     from nltk.text import TextCollection
#
#     tweets = [t[0] for t in db.execute('select tweet_text from tweets')]
#     return TextCollection(tweets)

def preprocess_text(text):
    # https://towardsdatascience.com/the-real-world-as-seen-on-twitter-sentiment-analysis-part-one-5ac2d06b63fb
    import re
    text = re.sub(r'\&\w*;', '', text) # Remove HTML special entities (e.g. &amp;)
    text = re.sub('@[^\s]+', '', text) # Convert @username to AT_USER
    text = re.sub(r'\$\w*', '', text) # Remove tickers
    text = text.lower() # To lowercase
    text = re.sub(r'https?:\/\/.*\/\w*', '', text) # Remove hyperlinks
    text = re.sub(r'#\w*', '', text) # Remove hashtags
    text = re.sub(r'\b\w{1,2}\b', '', text) # Remove words with 2 or fewer letters
    return text

# def ngrams(n = 2, english_words_only = True, remove_stopwords = True, limit = None):
#     from collections import Counter
#     import nltk
#     import db
#     query = 'select tweet_text from tweets' if limit is None else 'select tweet_text from tweets limit ' + str(limit)
#     text = ' hylotheism pseudonymuncle '.join([t[0] for t in db.execute(query)])
#     text = preprocess_text(text)
#     tokens = nltk.word_tokenize(text)
#
#     if english_words_only:
#         words = set(nltk.corpus.words.words())
#         tokens = [w for w in tokens if w in words]
#     if remove_stopwords:
#         stopwords = set(nltk.corpus.stopwords.words('english'))
#         tokens = [w for w in tokens if w not in stopwords]
#
#     zip_grams = zip(*[tokens[i:] for i in range(n)])
#     clean_zip_grams = [tks for tks in zip_grams if 'hylotheism' not in tks and 'pseudonymuncle' not in tks]
#     ngram_counts = Counter(clean_zip_grams)
#     return ngram_counts

def trigram_match_three(trigram, textbooks):
    for textbook in textbooks:
        glossary = textbooks[textbook]
        for item in glossary:
            if all(word in item for word in trigram):
                return True
    return False

def bigram_match_two(bigram, textbooks):
    for textbook in textbooks:
        glossary = textbooks[textbook]
        for item in glossary:
            if all(word in item for word in bigram):
                return True
    return False

def trigram_match_two(trigram, textbooks):
    for textbook in textbooks:
        glossary = textbooks[textbook]
        for item in glossary:
            match_count = 0
            if trigram[0] in item:
                match_count += 1
            if trigram[1] in item:
                match_count += 1
            if trigram[2] in item:
                match_count += 1
            if match_count == 2: # do not overlap with match three
                return True
    return False

def trigram_match_one(trigram, textbooks):
    for textbook in textbooks:
        glossary = textbooks[textbook]
        for item in glossary:
            match_count = 0
            if trigram[0] in item:
                match_count += 1
            if trigram[1] in item:
                match_count += 1
            if trigram[2] in item:
                match_count += 1
            if match_count == 1: # do not overlap with match three and match two
                return True
    return False

def bigram_match_one(bigram, textbooks):
    for textbook in textbooks:
        glossary = textbooks[textbook]
        for item in glossary:
            if any(word in item for word in bigram) and not all(word in item for word in bigram): # do not overlap with match two
                return True
    return False

def nltk_bigram_collocation(limit = None, textbook_matcher = None):
    import nltk
    import db
    query = 'select tweet_text from tweets' if limit is None else 'select tweet_text from tweets limit ' + str(limit)
    text = ' hylotheism pseudonymuncle '.join([t[0] for t in db.execute(query)])
    text = preprocess_text(text)
    tokens = nltk.word_tokenize(text)

    tokens = [w for w in tokens if w.isalpha()] # remove punctuation
    stopwords = set(nltk.corpus.stopwords.words('english'))
    tokens = [w for w in tokens if w not in stopwords] # remove stopwords

    bigram_measures = nltk.collocations.BigramAssocMeasures()
    finder = nltk.collocations.BigramCollocationFinder.from_words(tokens)
    finder.apply_ngram_filter(lambda *w: 'hylotheism' in w or 'pseudonymuncle' in w)

    import textbook
    finder.apply_ngram_filter(lambda *w: textbook_matcher(w, textbook.textbooks))

    for it in finder.score_ngrams(bigram_measures.pmi):
        print(it)

def nltk_trigram_collocation(limit = None, textbook_matcher = None):
    import nltk
    import db
    query = 'select tweet_text from tweets' if limit is None else 'select tweet_text from tweets limit ' + str(limit)
    text = ' hylotheism pseudonymuncle '.join([t[0] for t in db.execute(query)])
    text = preprocess_text(text)
    tokens = nltk.word_tokenize(text)

    tokens = [w for w in tokens if w.isalpha()] # remove punctuation
    stopwords = set(nltk.corpus.stopwords.words('english'))
    tokens = [w for w in tokens if w not in stopwords] # remove stopwords

    trigram_measures = nltk.collocations.TrigramAssocMeasures()
    finder = nltk.collocations.TrigramCollocationFinder.from_words(tokens)
    finder.apply_ngram_filter(lambda *w: 'hylotheism' in w or 'pseudonymuncle' in w)

    import textbook
    finder.apply_ngram_filter(lambda *w: textbook_matcher(w, textbook.textbooks))

    for it in finder.score_ngrams(trigram_measures.pmi):
        print(it)

if __name__ == '__main__':
    import sys
    import pprint

    limit = int(sys.argv[1]) if len(sys.argv) >= 2 else None
    nltk_trigram_collocation(limit = limit, textbook_matcher = trigram_match_three)
    print()
    nltk_trigram_collocation(limit = limit, textbook_matcher = trigram_match_two)
    print()
    nltk_trigram_collocation(limit = limit, textbook_matcher = trigram_match_one)
    print()

    nltk_bigram_collocation(limit = limit, textbook_matcher = bigram_match_two)
    print()
    nltk_bigram_collocation(limit = limit, textbook_matcher = bigram_match_one)
    print()

#   if len(sys.argv) >= 2 and sys.argv[1] == 'ngrams':
#       if len(sys.argv) >= 3:
#           n = int(sys.argv[2])
#           limit = int(sys.argv[3]) if len(sys.argv) >= 4 else None
#           counts = ngrams(n = n, limit = limit)
#           pprint.pprint(counts.most_common(1000))

