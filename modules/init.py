import asyncio
from aiofiles import os
import importlib.util
import sys

from modules import read, request, merge, create, locate
from modules.temporaries import temporaries


async def check(dask_client, requester: str, subscriber: dict, layer: int, port: int, latest_tessellation_version: str, all_supported_clusters_data: list[dict], history_dataframe, dt_start, configuration: dict) -> dict:
    node_data = create.snapshot(requester, subscriber, port, layer, latest_tessellation_version, dt_start)
    node_data = locate.node_cluster(node_data, all_supported_clusters_data)
    historic_node_dataframe = await locate.historic_node_data(dask_client, node_data, history_dataframe)
    historic_node_dataframe = locate.former_historic_node_data(historic_node_dataframe)
    node_data = merge.historic_data(node_data, historic_node_dataframe)
    node_data = merge.cluster_agnostic_node_data(node_data, all_supported_clusters_data)
    node_data = await request.node_cluster_data_from_dynamic_module(node_data, configuration)
    node_data = temporaries.run(node_data, all_supported_clusters_data)

    return node_data


async def run(dask_client, requester, dt_start, latest_tessellation_version: str, all_supported_cluster_data: list[dict], configuration: dict) -> list:
    subscriber_futures = []
    request_futures = []
    history_dataframe = await read.history(configuration)
    subscriber_dataframe = await read.subscribers(configuration)
    if requester is not None:
        ips = list(set(await dask_client.compute(subscriber_dataframe["ip"][subscriber_dataframe["name"] == requester].values)))
    else:
        ips = list(set(await dask_client.compute(subscriber_dataframe["ip"].values)))
    for ip in ips:
        subscriber_futures.append(asyncio.create_task(locate.registered_subscriber_node_data(dask_client, ip, subscriber_dataframe)))
    for fut in subscriber_futures:
        subscriber = await fut
        for k, v in subscriber.items():
            if k == "public_l0":
                for port in v:
                    layer = 0
                    request_futures.append(asyncio.create_task(check(dask_client, requester, subscriber, layer, port, latest_tessellation_version, all_supported_cluster_data, history_dataframe, dt_start, configuration)))
            elif k == "public_l1":
                for port in v:
                    layer = 1
                    request_futures.append(asyncio.create_task(check(dask_client, requester, subscriber, layer, port, latest_tessellation_version, all_supported_cluster_data, history_dataframe, dt_start, configuration)))

    return request_futures


async def send(bot, data, configuration):
    futures = []
    for node_data in data:
        if node_data["notify"] is True:
            if node_data["state"] != "offline":
                cluster_name = node_data["clusterNames"]
            else:
                cluster_name = node_data["formerClusterNames"]

            if await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{cluster_name}.py"):
                spec = importlib.util.spec_from_file_location(f"{cluster_name}.build_embed",
                                                              f"{configuration['file settings']['locations']['cluster modules']}/{cluster_name}.py")
                module = importlib.util.module_from_spec(spec)
                sys.modules[f"{cluster_name}.build_embed"] = module
                spec.loader.exec_module(module)
                embed = module.build_embed(node_data)

                futures.append(asyncio.create_task(bot.get_channel(int(977357753947402281)).send(embed=embed)))
    for fut in futures:
        await fut



