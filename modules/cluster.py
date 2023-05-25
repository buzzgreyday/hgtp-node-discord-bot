from aiofiles import os

from modules import determine_module
from modules.discord import discord


def locate_node(node_data, all_cluster_data):
    for cluster_data_idx, lst_of_clusters in enumerate(all_cluster_data):
        for cluster_idx, cluster in enumerate(lst_of_clusters):
            if int(node_data['layer']) == int(cluster["layer"].split(' ')[-1]):
                for peer in cluster["peer data"]:
                    if (peer["ip"] == node_data["host"]) and (peer["id"] == node_data["id"]):
                        return cluster


async def get_module_data(process_msg, node_data, configuration):

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