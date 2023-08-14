import asyncio
import logging
import threading
import time

import requests
from aiofiles import os

from assets.src import api, determine_module, schemas


class VersionManager:
    def __init__(self, configuration):
        self.lock = threading.Lock()
        self.configuration = configuration
        self.version = self.check_github_version()  # Set the initial version in __init__

    def update_version(self):
        while True:
            # Sets the version to the latest, every 30 seconds
            with self.lock:
                self.version = self.check_github_version()
                print(self.version)
            time.sleep(30)

    def check_github_version(self):
        # Actual version check logic here
        i = 0
        while True:
            i += 1
            sleep = 3 ** i
            data = requests.get(
                f"{self.configuration['tessellation']['github']['url']}/"
                f"{self.configuration['tessellation']['github']['releases']['latest']}")
            if data is not None:
                data = data.json()
                return data["tag_name"][1:]
            else:
                logging.getLogger(__name__).warning(
                    f"preliminaries.py - {self.configuration['tessellation']['github']['url']}/{self.configuration['tessellation']['github']['releases']['latest']} not reachable; forcing retry in {sleep} seconds")
                time.sleep(sleep)

    def get_version(self):
        # Returns the version
        with self.lock:
            print(f"Got version: {self.version}")
            return self.version


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