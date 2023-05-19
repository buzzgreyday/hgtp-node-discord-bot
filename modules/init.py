import asyncio
from aiofiles import os
import importlib.util
import sys

from modules import read, request, merge, create, locate, determine_module
from modules.discord import discord
from modules.temporaries import temporaries


async def check(dask_client, bot, process_msg, requester: str, subscriber: dict, layer: int, port: int, latest_tessellation_version: str, all_supported_clusters_data: list[dict], history_dataframe, dt_start, configuration: dict) -> tuple:
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

    return node_data, process_msg


async def process(dask_client, bot, process_msg, requester, dt_start, latest_tessellation_version: str, all_supported_cluster_data: list[dict], configuration: dict) -> list:
    async def locate_ips(requester):
        if requester is None:
            return list(set(await dask_client.compute(subscriber_dataframe["ip"])))
        else:
            return list(set(await dask_client.compute(subscriber_dataframe["ip"][subscriber_dataframe["contact"].astype(dtype=int) == int(requester)])))

    subscriber_futures = []
    request_futures = []
    history_dataframe = await read.history(configuration)
    subscriber_dataframe = await read.subscribers(configuration)
    ips = await locate_ips(requester)
    for ip in ips:
        subscriber_futures.append(asyncio.create_task(locate.registered_subscriber_node_data(dask_client, bot, ip, subscriber_dataframe)))

    for fut in subscriber_futures:
        subscriber = await fut
        for k, v in subscriber.items():
            if k == "public_l0":
                for port in v:
                    layer = 0
                    request_futures.append(asyncio.create_task(check(dask_client, bot, process_msg, requester, subscriber, layer, port, latest_tessellation_version, all_supported_cluster_data, history_dataframe, dt_start, configuration)))
            elif k == "public_l1":
                for port in v:
                    layer = 1
                    request_futures.append(asyncio.create_task(check(dask_client, bot, process_msg, requester, subscriber, layer, port, latest_tessellation_version, all_supported_cluster_data, history_dataframe, dt_start, configuration)))

    return request_futures


async def send(ctx, process_msg, bot, data, configuration):
    futures = []
    for node_data in data:
        if node_data["notify"] is True:
            if node_data["state"] != "offline":
                cluster_name = node_data["clusterNames"]
            else:
                cluster_name = node_data["formerClusterNames"]
            if await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{cluster_name}.py"):
                module = determine_module.set_module(cluster_name, configuration)
                embed = module.build_embed(node_data)
                if process_msg is not None:
                    futures.append((asyncio.create_task(ctx.author.send(embed=embed))))
                elif process_msg is None:
                    futures.append(asyncio.create_task(bot.get_channel(977357753947402281).send(embed=embed)))
    for fut in futures:
        await fut



