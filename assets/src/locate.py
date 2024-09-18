from typing import List

from assets.src import schemas


async def id_offline(layer, name, configuration):
    return configuration["modules"][name][layer]["id"]


def node(node_data: schemas.Node, cluster_data: schemas.Cluster):
    """THIS IS THE REASON A CLUSTER CAN COME OUT AS NONE!!! This function loops through all cluster data supported by the bot and returns the relevant cluster data"""
    found = False
    former_cluster_data = None
    if node_data is not None:
        for val in (node_data.former_cluster_name, node_data.last_known_cluster_name):
            if val is not None:
                former_cluster_name = val
                break
            else:
                former_cluster_name = None
        if cluster_data.layer == node_data.layer:
            # if binary_node_search(node_data, cluster_data.peer_data):
            if search(node_data, cluster_data.peer_data):
                found = True
                return found, cluster_data
            elif former_cluster_name == cluster_data.name:
                former_cluster_data = cluster_data
            else:
                former_cluster_data = None
    return found, former_cluster_data


def search(node_data, cluster_peer_data):
    for peer_id in cluster_peer_data:
        if node_data.id == peer_id:
            return True
    return False


def binary_node_search(node_data: schemas.Node, peer_data: List[dict]):
    """This function does a binary search to see if the Node ID is a peer in a cluster supported by the bot."""
    start = 0
    end = len(peer_data) - 1

    while start <= end:

        mid = (start + end) // 2
        peer = peer_data[mid]

        if (
            peer["id"] == node_data.id
            and peer["ip"] == node_data.ip
            and peer["publicPort"] == node_data.public_port
        ):
            return True

        elif (
            node_data.id < peer["id"]
        ):
            end = mid - 1
        else:
            start = mid + 1

    return False
