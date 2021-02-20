import db # refers to ../db.py

connection_as_follower = db.connection_as_follower_count()
connection_as_following = db.connection_as_following_count()
print(len(connection_as_follower), 'unique connection among followers')
print(len(connection_as_following), 'unique connection among followings')

top = lambda n = 0: connection_as_follower[:n] if n > 0 else connection_as_follower
at_least_as_follower = lambda n = 0: [t for t in connection_as_follower if t[1] >= n] # find connection users who follow at least `n` seed users
at_least_as_following = lambda n = 0: [t for t in connection_as_following if t[1] >= n] # find connection users who is followed by at least `n` seed users

if __name__ == '__main__':
    import csv

    connection_as_following_map = dict(connection_as_following)
    connection_map = db.mk_connection_user_map()

    with open('connection_as_follower.csv', 'w', newline = '') as f:
        writer = csv.writer(f)
        writer.writerow(('connection', 'username', 'name', 'seed_users_you_follow', 'seed_users_following_you'))
        for connection, count in [t for t in connection_as_follower if t[1] >= 5]:
            username, name, _1, _2 = connection_map[connection]
            writer.writerow((connection, username, name, count, connection_as_following_map[connection] if connection in connection_as_following_map else 0))

    with open('connection_as_following.csv', 'w', newline = '') as f:
        writer = csv.writer(f)
        writer.writerow(('connection', 'username', 'name', 'seed_users_following_you'))
        for connection, count in [t for t in connection_as_following if t[1] >= 5]:
            username, name, _1, _2 = connection_map[connection]
            writer.writerow((connection, username, name, count))

