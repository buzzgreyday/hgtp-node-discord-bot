from aiofiles import os

from modules import determine_module
from modules.discord import discord


def locate_cluster(node_data, all_supported_clusters_data):
    for lst in all_supported_clusters_data:
        for cluster in lst:
            if int(node_data['layer']) == int(cluster["layer"].split(' ')[-1]):
                for peer in cluster["peer data"]:
                    # LATER INCLUDE ID WHEN SUBSCRIBING
                    if (peer["ip"] == node_data["host"]) and (peer["id"] == node_data["id"]):
                        node_data["clusterNames"] = cluster["cluster name"].lower()
                        node_data["latestClusterSession"] = cluster["cluster session"]
                        node_data["clusterVersion"] = cluster["version"]
                        # Because we know the IP and ID, we can auto-recognize changing publicPort and later update
                        # the subscription based on the node_data values
                        if node_data["publicPort"] != peer["publicPort"]:
                            node_data["publicPort"] = peer["publicPort"]
    return node_data


def data_template(requester, subscriber, port: int, layer: int, latest_tessellation_version: str, dt_start):
    return {
        "name": subscriber["name"][subscriber.public_port == port].values[0],
        "contact": subscriber["contact"][subscriber.public_port == port].values[0],
        "host": subscriber["ip"][subscriber.public_port == port].values[0],
        "layer": layer,
        "publicPort": port,
        "p2pPort": None,
        "id": subscriber["id"][subscriber.public_port == port].values[0],
        "nodeWalletAddress": None,
        "nodeWalletBalance": None,
        "clusterNames": None,
        "formerClusterNames": None,
        "state": None,
        "clusterState": None,
        "formerClusterState": None,
        "clusterConnectivity": None,
        "formerClusterConnectivity": None,
        "rewardState": None,
        "formerRewardState": None,
        "rewardTrueCount": None,
        "rewardFalseCount": None,
        "clusterAssociationTime": None,
        "formerClusterAssociationTime": None,
        "clusterDissociationTime": None,
        "formerClusterDissociationTime": None,
        "nodeClusterSession": None,
        "latestClusterSession": None,
        "nodePeerCount": None,
        "clusterPeerCount": None,
        "formerClusterPeerCount": None,
        "version": None,
        "clusterVersion": None,
        "latestVersion": latest_tessellation_version,
        "cpuCount": None,
        "diskSpaceTotal": None,
        "diskSpaceFree": None,
        "1mSystemLoadAverage": None,
        "notify": False if requester is None else True,
        "lastNotifiedTimestamp": None,
        "timestampIndex": dt_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "formerTimestampIndex": None

    }


def merge_general_cluster_data(node_data, all_supported_clusters_data):

    for lst in all_supported_clusters_data:
        for cluster in lst:
            if cluster["layer"] == f"layer {node_data['layer']}":
                if cluster["cluster name"] == node_data["clusterNames"]:
                    node_data["clusterPeerCount"] = cluster["peer count"]
                    node_data["clusterState"] = cluster["state"]
                if cluster["cluster name"] == node_data["formerClusterNames"]:
                    node_data["formerClusterPeerCount"] = cluster["peer count"]
                    node_data["formerClusterState"] = cluster["state"]

    return node_data


async def get_cluster_module_data(process_msg, node_data, configuration):

    if await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{node_data['clusterNames']}.py"):
        process_msg = await discord.update_request_process_msg(process_msg, 4, f"{node_data['clusterNames']} layer {node_data['layer']}")
        module = determine_module.set_module(node_data['clusterNames'], configuration)
        node_data = await module.node_cluster_data(node_data, configuration)
        return node_data, process_msg
    elif await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{node_data['formerClusterNames']}.py"):
        process_msg = await discord.update_request_process_msg(process_msg, 4, f"{node_data['clusterNames']} layer {node_data['layer']}")
        module = determine_module.set_module(node_data['formerClusterNames'], configuration)
        node_data = await module.node_cluster_data(node_data, configuration)
        return node_data, process_msg
    else:
        return node_data, process_msg