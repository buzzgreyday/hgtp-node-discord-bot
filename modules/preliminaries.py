import asyncio

import aiofiles
import yaml
from aiofiles import os

from modules import determine_module, api


async def latest_version_github(configuration):
    data = await api.safe_request(
        f"{configuration['request']['url']['github']['api repo url']}/"
        f"{configuration['request']['url']['github']['url endings']['tessellation']['latest release']}", configuration)
    return data["tag_name"][1:]


async def supported_clusters(cluster_layer: str, cluster_names: dict, configuration: dict) -> list:

    all_clusters_data = []
    for cluster_name, cluster_info in cluster_names.items():
        for lb_url in cluster_info["url"]:
            if await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{cluster_name}.py"):
                module = determine_module.set_module(cluster_name, configuration)
                cluster = await module.request_cluster_data(lb_url, cluster_layer, cluster_name, configuration)
                all_clusters_data.append(cluster)
    del lb_url, cluster_name, cluster_info
    return all_clusters_data


async def get(configuration):
    tasks = []
    cluster_data = []
    latest_tessellation_version = await latest_version_github(configuration)
    for cluster_layer, cluster_names in list(configuration["request"]["url"]["clusters"]["load balancer"].items()):
        tasks.append(asyncio.create_task(supported_clusters(cluster_layer, cluster_names, configuration)))
    for task in tasks:
        cluster_data.append(await task)
    """RELOAD CONFIGURATION"""
    async with aiofiles.open('data/config.yml', 'r') as file:
        configuration = yaml.safe_load(await file.read())
    return configuration, cluster_data, latest_tessellation_version
