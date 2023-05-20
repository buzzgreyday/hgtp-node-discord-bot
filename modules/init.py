import asyncio
from aiofiles import os
import importlib.util
import sys

from modules import read, request, merge, create, locate, determine_module, subscription
from modules.discord import discord
from modules.temporaries import temporaries


async def check(dask_client, bot, process_msg, requester, subscriber, port, layer, latest_tessellation_version: str,
                history_dataframe, all_supported_clusters_data: list[dict], dt_start, configuration: dict) -> tuple:
    process_msg = await discord.update_request_process_msg(process_msg, 2, None)
    node_data = create.snapshot(requester, subscriber, port, layer, latest_tessellation_version, dt_start)
    node_data = locate.node_cluster(node_data, all_supported_clusters_data)
    historic_node_dataframe = await locate.historic_node_data(dask_client, node_data, history_dataframe)
    historic_node_dataframe = locate.former_historic_node_data(historic_node_dataframe)
    node_data = merge.historic_data(node_data, historic_node_dataframe)
    node_data = merge.cluster_agnostic_node_data(node_data, all_supported_clusters_data)
    process_msg = await discord.update_request_process_msg(process_msg, 3, None)
    node_data, process_msg = await request.node_cluster_data_from_dynamic_module(process_msg, node_data, configuration)
    node_data = temporaries.run(node_data, all_supported_clusters_data)
    await subscription.update_public_port(dask_client, node_data)

    return node_data, process_msg
