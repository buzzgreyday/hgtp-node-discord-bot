import asyncio
import logging
from pathlib import Path

from aiofiles import os

from assets.src import api, determine_module, schemas

WORKING_DIR = f'{str(Path.home())}/bot'


async def latest_version_github(configuration):
    i = 0
    while True:
        i += 1
        sleep = 3 * i
        data = await api.safe_request(
            f"{configuration['tessellation']['github']['url']}/"
            f"{configuration['tessellation']['github']['releases']['latest']}", configuration)
        if data is not None:
            return data["tag_name"][1:]
        else:
            logging.getLogger(__name__).warning(f"preliminaries.py - {configuration['tessellation']['github']['url']}/{configuration['tessellation']['github']['releases']['latest']} not reachable; forcing retry in {sleep} seconds")
            await asyncio.sleep(sleep)
            await latest_version_github(configuration)


async def supported_clusters(name: str, layer: int, configuration: dict) -> schemas.Cluster:
    url = configuration["modules"][name][layer]["url"][0]
    if await os.path.exists(f"{WORKING_DIR}/{configuration['file settings']['locations']['cluster modules']}/{name}.py"):
        module = determine_module.set_module(name, configuration)
        cluster = await module.request_cluster_data(url, layer, name, configuration)
        return cluster
