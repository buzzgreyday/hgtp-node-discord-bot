import logging
import threading
import time
import traceback

import requests
from aiofiles import os

from assets.src import determine_module, schemas
from assets.src.database.database import api as database_api
import uvicorn


"""THREADS"""

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
            time.sleep(180)

    def check_github_version(self):
        # Actual version check logic here
        i = 0
        while True:
            i += 1
            sleep = 20 * i
            data = requests.get(
                f"{self.configuration['tessellation']['github']['url']}/"
                f"{self.configuration['tessellation']['github']['releases']['latest']}")
            if data:
                data = data.json()
                try:
                    version = data["tag_name"][1:]
                except KeyError:
                    logging.getLogger(__name__).warning(
                        f"preliminaries.py - {self.configuration['tessellation']['github']['url']}/{self.configuration['tessellation']['github']['releases']['latest']} KeyError: forcing retry in {3660} seconds\n\t{traceback.print_exc()}")
                    time.sleep(3660)
                else:
                    return version
            else:
                logging.getLogger(__name__).warning(
                    f"preliminaries.py - {self.configuration['tessellation']['github']['url']}/{self.configuration['tessellation']['github']['releases']['latest']} not reachable; forcing retry in {sleep} seconds")
                time.sleep(sleep)

    def get_version(self):
        # Returns the version
        with self.lock:
            return self.version


def run_uvicorn():
    host = "127.0.0.1"
    port = 8000
    log_level = 'debug'
    logging.getLogger(__name__).info(f"main.py - Uvicorn running on {host}:{port}")
    uvicorn.run(database_api, host=host, port=port, log_level=log_level, log_config=f'assets/data/logs/bot/uvicorn.ini')


"""REQUEST DATA"""


async def supported_clusters(name: str, layer: int, configuration: dict) -> schemas.Cluster:
    url = configuration["modules"][name][layer]["url"][0]
    if await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{name}.py"):
        module = determine_module.set_module(name, configuration)
        cluster = await module.request_cluster_data(url, layer, name, configuration)
        return cluster


"""RUNTIME"""


def generate_runtimes(configuration) -> list:
    from datetime import datetime, timedelta

    start = datetime.strptime(configuration['general']['loop_init_time'], "%H:%M:%S")
    end = (datetime.strptime(configuration['general']['loop_init_time'], "%H:%M:%S") + timedelta(hours=24))
    return [
        (start + timedelta(hours=(configuration['general']['loop_interval_minutes']) * i / 60)).strftime("%H:%M:%S")
        for i in
        range(int((end - start).total_seconds() / 60.0 / (configuration['general']['loop_interval_minutes'])))]