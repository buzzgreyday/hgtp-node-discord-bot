from aiofiles import os

from modules import subscription, history, determine_module
from modules.discord import discord


def locate_cluster_data(node_data, cluster_data):
    for cluster_data_idx, lst_of_clusters in enumerate(cluster_data):
        for cluster_idx, cluster in enumerate(lst_of_clusters):
            if int(node_data['layer']) == int(cluster["layer"].split(' ')[-1]):
                for peer in cluster["peer data"]:
                    if (peer["ip"] == node_data["host"]) and (peer["id"] == node_data["id"]):
                        return cluster


def merge_cluster_data(node_data, cluster):

    """for lst_of_clusters in cluster_data:
        for cluster in lst_of_clusters:"""
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


def merge_general_cluster_data(node_data, cluster):
    """for lst_of_clusters in cluster_data:
        for cluster in lst_of_clusters:"""
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


async def check(dask_client, bot, process_msg, requester, subscriber, port, layer, latest_tessellation_version: str,
                history_dataframe, cluster_data: list[dict], dt_start, configuration: dict) -> tuple:
    process_msg = await discord.update_request_process_msg(process_msg, 2, None)
    node_data = data_template(requester, subscriber, port, layer, latest_tessellation_version, dt_start)
    cluster = locate_cluster_data(node_data, cluster_data)
    node_data = merge_cluster_data(node_data, cluster)
    historic_node_dataframe = await history.node_data(dask_client, node_data, history_dataframe)
    historic_node_dataframe = history.former_node_data(historic_node_dataframe)
    node_data = history.merge(node_data, historic_node_dataframe)
    node_data = merge_general_cluster_data(node_data, cluster)
    process_msg = await discord.update_request_process_msg(process_msg, 3, None)
    node_data, process_msg = await get_cluster_module_data(process_msg, node_data, configuration)
    node_data = determine_module.set_module(node_data["clusterNames"], configuration).check_rewards(node_data, cluster)
    await subscription.update_public_port(dask_client, node_data)

    return node_data, process_msg