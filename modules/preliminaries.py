import asyncio

import aiofiles
import yaml
from aiofiles import os

from modules import determine_module, api


async def latest_version_github(configuration):
    print(f"{configuration['tessellation']['github']['url']}/"
        f"{configuration['tessellation']['github']['releases']['latest']}")
    data = await api.safe_request(
        f"{configuration['tessellation']['github']['url']}/"
        f"{configuration['tessellation']['github']['releases']['latest']}", configuration)
    print(data["tag_name"][1:])
    return data["tag_name"][1:]


async def supported_clusters(name: str, layers: list, configuration: dict) -> list:
    data = []
    for L in layers:
        url = configuration["modules"][name][L]["url"][0]
        if await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{name}.py"):
            module = determine_module.set_module(name, configuration)
            cluster = await module.request_cluster_data(url, L, name, configuration)
            data.append(cluster)
            print(data)
    return data


async def cluster_data(configuration):
    tasks = []
    all_cluster_data = []
    for cluster in list(configuration["modules"].items()):
        name = cluster[0]
        layers = cluster[1]
        tasks.append(asyncio.create_task(supported_clusters(name, layers, configuration)))
    for task in tasks:
        all_cluster_data.append(await task)
    print(all_cluster_data)
    return all_cluster_data
