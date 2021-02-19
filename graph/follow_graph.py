import networkx as nx # use networkx to construct graph
import csv

# filter connection users
import filter_connections # refers to `../connections/filter_connections.py`
connections_as_follower_min5 = filter_connections.at_least_as_follower(5) # connection users who follow at least 5 seed users
connections_as_following_min5 = filter_connections.at_least_as_following(5) # connection users who are followed by at least 5 seed users
filtered_connections_as_follower_map = dict(connections_as_follower_min5)
filtered_connections_as_following_map = dict(connections_as_following_min5)

# get seed ~ connection user connections
import db # refers to `../db.py`
users = db.seed_users()
followers = db.followers()
followings = db.followings()

# take only filtered connection users connections
filtered_followers = [t for t in followers if t[1] in filtered_connections_as_follower_map]
filtered_followings = [t for t in followings if t[1] in filtered_connections_as_following_map]
print(len(filtered_followers), 'filtered seed user ~ follower connections')
print(len(filtered_followings), 'filtered seed user ~ following connections')

# vertex set
user_set = set([u[1] for u in users])
follower_set = set([r[1] for r in filtered_followers])
following_set = set([r[1] for r in filtered_followings])
total_set = user_set.union(follower_set, following_set)
print(len(total_set), 'unified filtered users in question')

# build networkx graph
G = nx.Graph()
G.add_nodes_from(total_set)
G.add_edges_from(filtered_followers)
G.add_edges_from(filtered_followings)
print('Constructed graph with', G.number_of_nodes(), 'nodes and', G.number_of_edges(), 'edges')
deg = lambda node: G.degree(node)

def write(obj, f, header=None):
    """ Write data to csv """
    with open(f, 'w', newline = '') as csvf:
        writer = csv.writer(csvf)
        if header is not None:
            writer.writerow(header)
        writer.writerows(obj)

def louvain_partition(write_csv=False):
    """ Generate a louvain partitions set for the graph
    This version of code produces the partitions saved in `louvain-4-sorted-deg-filtered-conn` """
    import community as community_louvain # package to find partitions
    partition = community_louvain.best_partition(G)
    parts = set(partition.values())
    print('Louvain finds', len(parts), 'partitions')

    seed_user_map = db.mk_seed_user_map()
    connection_user_map = db.mk_connection_user_map()
    user_map = {**seed_user_map, **connection_user_map} # user maps contain key information for each user including follower counts, etc.
    members = {}
    for part in parts:
        us = [(u, *(user_map[u] if u in user_map else (None, None, None, None)), deg(u)) for u in partition.keys() if partition[u] == part]
        members[part] = sorted(us, key = lambda t: t[-1], reverse = True) # sorted by degree (unique pair connections)
    if write_csv:
        for part in members:
            write(members[part], 'louvain-partition-' + str(part) + '.csv', ('id', 'username', 'name', 'followers', 'friends', 'deg'))
    return members

