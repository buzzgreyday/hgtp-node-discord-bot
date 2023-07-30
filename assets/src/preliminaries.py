import asyncio
from pathlib import Path

from aiofiles import os

from assets.src import api, determine_module

WORKING_DIR = f'{str(Path.home())}/bot'


async def latest_version_github(configuration):
    data = await api.safe_request(
        f"{configuration['tessellation']['github']['url']}/"
        f"{configuration['tessellation']['github']['releases']['latest']}", configuration)
    vers = data["tag_name"][1:] if data is not None else latest_version_github(configuration)
    return vers


async def supported_clusters(name: str, layer: int, configuration: dict) -> list:
    url = configuration["modules"][name][layer]["url"][0]
    if await os.path.exists(f"{WORKING_DIR}/{configuration['file settings']['locations']['cluster modules']}/{name}.py"):
        module = determine_module.set_module(name, configuration)
        cluster = await module.request_cluster_data(url, layer, name, configuration)
        return cluster


async def cluster_data(configuration):
    tasks = []
    all_cluster_data = []
    for name in configuration["modules"].keys():
        for layer in configuration["modules"][name].keys():
            tasks.append(asyncio.create_task(supported_clusters(name, layer, configuration)))
    for task in tasks:
        all_cluster_data.append(await task)
    return all_cluster_data
