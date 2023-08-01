# Third-party imports
import logging

from aiofiles import os
from typing import List

from assets.src import schemas, determine_module
from assets.src.discord import discord


def locate_node_binary(node_data: schemas.Node, peer_data: List[dict]):
    """This function does a binary search to see if the Node ID is a peer in a cluster supported by the bot."""
    start = 0
    end = len(peer_data) - 1

    while start <= end:

        mid = (start + end) // 2
        peer = peer_data[mid]
        if peer["id"] == node_data.id and peer["ip"] == node_data.ip and peer["publicPort"] == node_data.public_port:
            return True

        if node_data.id < peer["id"]:
            end = mid - 1
        else:
            start = mid + 1

    return False


async def locate_id_offline(layer, name, configuration):
    return configuration["modules"][name][layer]["id"]


def locate_node(node_data: schemas.Node, all_cluster_data: List[dict]):
    """THIS IS THE REASON A CLUSTER CAN COME OUT AS NONE!!! This function loops through all cluster data supported by the bot and returns the relevant cluster data"""
    found = False
    for val in (node_data.former_cluster_name, node_data.last_known_cluster_name):
        if val is not None:
            former_cluster = val
            break
        else:
            former_cluster = None
    for cluster in all_cluster_data:
        if cluster["layer"] == node_data.layer:
            if locate_node_binary(node_data, cluster["peer_data"]):
                found = True
                return found, cluster
            if former_cluster == cluster["name"]:
                former_cluster = cluster
    return found, former_cluster
    # Changed this


async def get_module_data(process_msg, node_data: schemas.Node, configuration):
    if await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{node_data.cluster_name}.py"):
        process_msg = await discord.update_request_process_msg(process_msg, 4, f"{node_data.cluster_name} layer {node_data.layer}")
        module = determine_module.set_module(node_data.cluster_name, configuration)
        node_data = await module.node_cluster_data(node_data, node_data.cluster_name,configuration)

        return node_data, process_msg

    elif await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{node_data.former_cluster_name}.py"):
        process_msg = await discord.update_request_process_msg(process_msg, 4, f"{node_data.former_cluster_name} layer {node_data.layer}")
        module = determine_module.set_module(node_data.former_cluster_name, configuration)
        node_data = await module.node_cluster_data(node_data, node_data.former_cluster_name,configuration)

        return node_data, process_msg

    elif await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{node_data.last_known_cluster_name}.py"):
        process_msg = await discord.update_request_process_msg(process_msg, 4, f"{node_data.last_known_cluster_name} layer {node_data.layer}")
        module = determine_module.set_module(node_data.last_known_cluster_name, configuration)
        node_data = await module.node_cluster_data(node_data, node_data.last_known_cluster_name, configuration)

        return node_data, process_msg

    else:
        logging.getLogger(__name__).warning(f"cluster.py - No module found while processing:\n{node_data.dict()}")
        return node_data, process_msg
