from aiofiles import os
import time
from modules import tessellation, determine_module
from modules.discord import discord
import asyncio
import aiofiles
import aiohttp
from aiohttp import client_exceptions
from datetime import datetime
import importlib.util
import sys
import yaml
import logging


class Request:
    def __init__(self, url):
        self.url = url

    async def json(self, configuration):
        timeout = aiohttp.ClientTimeout(total=configuration["request"]["timeout"])
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.debug(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST SUCCEEDED")
                    return data
                elif resp.status == 503:
                    logging.debug(
                        f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED: SERVICE UNAVAILABLE")
                    return 503
                else:
                    logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED")
            del resp
            await session.close()

    async def text(self, strings: list | tuple, configuration: dict):
        async def find_in_text(text, strings: list | tuple):
            results = []

            for line in text.split('\n'):
                if not line.startswith('#'):
                    for i, item in enumerate(strings):
                        idx = text.find(item)
                        line_start = text[idx:].split('\n')
                        value = line_start[0].split(' ')[1]
                        results.append(value)
                        del (value, line_start, idx)
                        if i >= len(strings):
                            break
                    break
            return results

        timeout = aiohttp.ClientTimeout(total=configuration["request"]["timeout"])
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, timeout=timeout) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    logging.debug(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST SUCCEEDED")
                    strings = await find_in_text(text, strings)
                    del text
                    return strings
                elif resp.status == 503:
                    logging.debug(
                        f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED: SERVICE UNAVAILABLE")
                    return 503
                else:
                    logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED")
            del resp
            await session.close()


async def safe(request_url: str, configuration: dict):
    data = None
    retry_count = 0
    run_again = True
    while run_again:
        try:
            if "metrics" in request_url.split("/"):
                strings = ('process_uptime_seconds{application=',
                           'system_cpu_count{application=',
                           'system_load_average_1m{application=',
                           'disk_free_bytes{application=',
                           'disk_total_bytes{application=',
                           )
                strings = await Request(request_url).text(strings, configuration)
                data = {
                    "clusterAssociationTime": strings[0],
                    "cpuCount": strings[1],
                    "1mSystemLoadAverage": strings[2],
                    "diskSpaceFree": strings[3],
                    "diskSpaceTotal": strings[4]
                }

            else:
                data = await Request(request_url).json(configuration)
            if retry_count >= configuration['request']['max retry count'] or data == 503:
                data = None
                break
            elif data is not None:
                break
            else:
                retry_count += 1
                await asyncio.sleep(configuration['request']['retry sleep'])
        except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientOSError,
                aiohttp.client_exceptions.ServerDisconnectedError) as e:
            if retry_count >= configuration['request']['max retry count']:
                data = None
                break
            retry_count += 1
            await asyncio.sleep(configuration['request']['retry sleep'])
            logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CLUSTER @ {request_url} UNREACHABLE - TRIED {retry_count}/{configuration['request']['max retry count']}")
        except (aiohttp.client_exceptions.ClientConnectorError, aiohttp.client_exceptions.InvalidURL):
            data = None
            break
    return data


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


async def preliminary_data(configuration):
    tasks = []
    cluster_data = []
    latest_tessellation_version = await tessellation.latest_version_github(configuration)
    for cluster_layer, cluster_names in list(configuration["request"]["url"]["clusters"]["load balancer"].items()):
        tasks.append(asyncio.create_task(supported_clusters(cluster_layer, cluster_names, configuration)))
    for task in tasks:
        cluster_data.append(await task)
    """RELOAD CONFIGURATION"""
    async with aiofiles.open('data/config.yml', 'r') as file:
        configuration = yaml.safe_load(await file.read())
    return configuration, cluster_data, latest_tessellation_version

