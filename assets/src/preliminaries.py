import asyncio
import logging
import threading
import time

from aiofiles import os

from assets.src import api, determine_module, schemas


class VersionManager:

    def __init__(self):
        self.version = None
        self.lock = threading.Lock

    def update_version(self, configuration):
        while True:
            time.sleep(30)
            with self.lock:
                # Sets the version to the latest, every 30 seconds
                self.version = self.check_github_version(configuration)

    def check_github_version(self, configuration):
        # Actual version check logic here
        i = 0
        while True:
            i += 1
            sleep = 3 ** i
            data = await api.safe_request(
                f"{configuration['tessellation']['github']['url']}/"
                f"{configuration['tessellation']['github']['releases']['latest']}", configuration)
            if data is not None:
                return data["tag_name"][1:]
            else:
                logging.getLogger(__name__).warning(
                    f"preliminaries.py - {configuration['tessellation']['github']['url']}/{configuration['tessellation']['github']['releases']['latest']} not reachable; forcing retry in {sleep} seconds")
                await asyncio.sleep(sleep)

    def get_version(self):
        # Returns the version
        with self.lock:
            return self.version

async def latest_version_github(configuration):
    i = 0
    while True:
        i += 1
        sleep = 3 ** i
        data = await api.safe_request(
            f"{configuration['tessellation']['github']['url']}/"
            f"{configuration['tessellation']['github']['releases']['latest']}", configuration)
        if data is not None:
            return data["tag_name"][1:]
        else:
            logging.getLogger(__name__).warning(f"preliminaries.py - {configuration['tessellation']['github']['url']}/{configuration['tessellation']['github']['releases']['latest']} not reachable; forcing retry in {sleep} seconds")
            await asyncio.sleep(sleep)


async def supported_clusters(name: str, layer: int, configuration: dict) -> schemas.Cluster:
    url = configuration["modules"][name][layer]["url"][0]
    if await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{name}.py"):
        module = determine_module.set_module(name, configuration)
        cluster = await module.request_cluster_data(url, layer, name, configuration)
        return cluster


def generate_runtimes(configuration) -> list:
    from datetime import datetime, timedelta

    start = datetime.strptime(configuration['general']['loop_init_time'], "%H:%M:%S")
    end = (datetime.strptime(configuration['general']['loop_init_time'], "%H:%M:%S") + timedelta(hours=24))
    return [
        (start + timedelta(hours=(configuration['general']['loop_interval_minutes']) * i / 60)).strftime("%H:%M:%S")
        for i in
        range(int((end - start).total_seconds() / 60.0 / (configuration['general']['loop_interval_minutes'])))]