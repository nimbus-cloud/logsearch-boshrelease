#/usr/bin/python3

import argparse
import curator
import datetime
import fcntl
import logging
import os
import sys


from elasticsearch import Elasticsearch
from time import sleep


###### Ensure script only runs onec - Start
fh = 0


def run_once():
    global fh
    fh = open(os.path.realpath(__file__), 'r')
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except:
        os._exit(0)


run_once()
###### Ensure script only runs onec - End

logging.basicConfig(format='[%(asctime)s] [%(lineno)d] - %(message)s', level=logging.WARN, datefmt='%Y-%m-%d %X %Z')
parser = argparse.ArgumentParser()
parser.add_argument("threshold", type=int, help="percentage where index pruning will take place")
parser.add_argument("ip", help="IP address of the eleasticseach API node")
parser.add_argument("skip", help="comma separated list of indices to skip")
args = parser.parse_args()
clusters = [
    {'env': 'default', 'ip': args.ip}
]

threshold = args.threshold
skip_arg = args.skip
skip_index = (skip_arg.replace(' ', '')).split(',')


class DataNode:

    def __init__(self, name, node_id, info):
        self.name = name
        self.node_id = node_id
        self.total_in_bytes = info['total_in_bytes']
        self.free_in_bytes = info['free_in_bytes']
        self.used_in_bytes = self.total_in_bytes - self.free_in_bytes
        self.indices = []

    def used_percentage(self):
        return (self.used_in_bytes / self.total_in_bytes) * 100

    def refresh(self, cluster):
        logging.warn("Current usage level for node %s: %f%%" % (self.name, (self.used_in_bytes / self.total_in_bytes) * 100))
        self.total_in_bytes = cluster.nodes.stats()['nodes'][self.node_id]['fs']['data'][0]['total_in_bytes']
        self.free_in_bytes = cluster.nodes.stats()['nodes'][self.node_id]['fs']['data'][0]['free_in_bytes']
        self.used_in_bytes = self.total_in_bytes - self.free_in_bytes
        self.indices = []
        logging.warn("Refreshed usage levels for node %s: %f%%" % (self.name, (self.used_in_bytes / self.total_in_bytes) * 100))


def get_nodes(cluster):
    node_list = []
    for node in cluster.nodes.stats()['nodes']:
        node_data = cluster.nodes.stats()['nodes'][node]
        if node_data['indices']['docs']['count'] > 0:
            node_info = node_data['fs']['data'][0]
            data_node = DataNode(name=node_data['name'], node_id=node, info=node_info)
            assign_index(cluster, data_node)
            node_list.append(data_node)

    return node_list


def assign_index(cluster, node):
    for index in cluster.cluster.state()['routing_nodes']['nodes'][node.node_id]:
        if index['index'] not in skip_index:
            if index['index'] not in node.indices:
                node.indices.append(index['index'])


def getKey(item):
    return item[1]


def identify_index_for_del(index_list, indices):
    indices_creation = []
    for index in index_list:
        index_tup = (index, indices.index_info[index]['age']['creation_date'])
        indices_creation.append(index_tup)

    return sorted(indices_creation, key=getKey)


def delete_index(cluster, del_index):
    logging.warn("Deleting index %s" % del_index)
    try:
        cluster.indices.delete(index=del_index)
    except Exception:
        logging.error("Can't delete index %s" % del_index)
    sleep(15)  # Allow time for cluster to normalise


def main():
    for cluster in clusters:
        logging.warn("Skip index includes: %s" % skip_index)
        logging.warn("Checking nodes in %s" % cluster['env'])
        env_cluster = Elasticsearch(hosts=cluster['ip'])
        nodes = get_nodes(env_cluster)
        indices = curator.IndexList(env_cluster)

        for node in nodes:
            if len(node.indices) > 0:
                sorted_indicies = identify_index_for_del(node.indices, indices)
                while node.used_percentage() > threshold:
                    old_index = sorted_indicies[0][0]
                    index_created = datetime.datetime.utcfromtimestamp(sorted_indicies[0][1])
                    logging.warn("Node %s over quota: %f" % (node.name, node.used_percentage()))
                    logging.warn("Oldest index on the node is %s created at %s" % (old_index, index_created))
                    delete_index(env_cluster, old_index)
                    sorted_indicies.pop(0)
                    node.refresh(cluster=env_cluster)
                else:
                    logging.warn("node %s under quota: %f%%" % (node.name, node.used_percentage()))
            else:
                logging.warn("no indices associated to node %s" % node.name)
        logging.warn('-----------------')


if __name__ == '__main__':
    main()
