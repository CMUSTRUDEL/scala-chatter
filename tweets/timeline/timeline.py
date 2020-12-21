import datetime
import db

def quarter(dt):
    return int((dt.month - 1) / 3)

def quarter_center(year, quar):
    """ return quasi-center date of the quarter, for charting purposes """
    month_map = [2, 5, 8, 11]
    return datetime.date(year, month_map[quar], 15)

def map_axis(min_dt, max_dt):
    begin_year, begin_quarter = min_dt.year, quarter(min_dt)
    end_year, end_quarter = max_dt.year, quarter(max_dt)

    year = begin_year
    quar = begin_quarter
    axis = 0
    axis_map = {}
    center_date_map = {}

    while True:
        axis_map[(year, quar)] = axis
        center_date_map[axis] = quarter_center(year, quar)
        if year == end_year and quar == end_quarter:
            return axis_map, center_date_map

        axis += 1
        quar += 1
        if quar >= 4:
            year += 1
            quar = 0

        if axis > 100:
            raise Exception('Time Axis Too Long')

def make(item_map, axis, top = None):
    """ item_map can be url/hashtag -> [(user_id, tweet_id)...]
    make item_time_map = url/hashtag -> [(user_id, tweet_id, created_at datetime, axis place)...] """
    min_dt, max_dt = axis
    axis_map, center_date_map = map_axis(min_dt, max_dt)
    print('Axis Map Done')

    time_table = db.execute(db.mk_connection(), 'select user_id, tweet_id, created_at from tweets')
    time_map = {}
    for user_id, tweet_id, dt in time_table:
        time_map[(user_id, tweet_id)] = dt
    print('Time Map Done')

    item_time_map = {}
    key_set = item_map.keys() if top is None else sorted(item_map.keys(), key = lambda k: len(item_map[k]), reverse = True)[:top]
    for item in key_set: # keys can be url/hashtag
        instances = item_map[item]
        time_instances = [(user_id, tweet_id, time_map[(user_id, tweet_id)]) for user_id, tweet_id in instances]
        time_instances = [(user_id, tweet_id, dt) for user_id, tweet_id, dt in time_instances if dt is not None]
        time_instances.sort(key = lambda t: t[2])
        item_time_map[item] = [(user_id, tweet_id, dt, axis_map[(dt.year, quarter(dt))]) for user_id, tweet_id, dt in time_instances]
        print('Done Item', item)
    return item_time_map, center_date_map

