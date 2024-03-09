import logging
import threading
import time
import traceback

import requests
from aiofiles import os

from assets.src import determine_module, schemas


"""THREADS"""


class VersionManager:
    def __init__(self, configuration):
        self.lock = threading.Lock()
        self.configuration = configuration
        self.version = (
            self.check_github_version()
        )  # Set the initial version in __init__

    def update_version(self):
        while True:
            # Sets the version to the latest, every 10 minutes
            with self.lock:
                self.version = self.check_github_version()
            time.sleep(600)

    def check_github_version(self):
        # Actual version check logic here
        while True:
            data = requests.get(
                f"{self.configuration['tessellation']['github']['url']}/"
                f"{self.configuration['tessellation']['github']['releases']['latest']}"
            )
            if data:
                data = data.json()
                try:
                    version = data["tag_name"][1:]
                    logging.getLogger("app").debug(
                        f"preliminaries.py - {self.configuration['tessellation']['github']['url']}/{self.configuration['tessellation']['github']['releases']['latest']} got version: {version}"
                    )
                except (KeyError, requests.exceptions.ConnectionError):
                    logging.getLogger("app").warning(
                        f"preliminaries.py - {self.configuration['tessellation']['github']['url']}/{self.configuration['tessellation']['github']['releases']['latest']} KeyError: forcing retry in {3600*2} seconds\n\t{traceback.print_exc()}"
                    )
                    time.sleep(600)
                else:
                    return version
            else:
                logging.getLogger("app").warning(
                    f"preliminaries.py - {self.configuration['tessellation']['github']['url']}/{self.configuration['tessellation']['github']['releases']['latest']} not reachable; forcing retry in {sleep} seconds"
                )
                time.sleep(600)

    def get_version(self):
        # Returns the version
        with self.lock:
            return self.version


"""REQUEST DATA"""


async def supported_clusters(
    session, name: str, layer: int, configuration: dict
) -> schemas.Cluster:
    url = configuration["modules"][name][layer]["url"][0]
    if await os.path.exists(
        f"{configuration['file settings']['locations']['cluster modules']}/{name}.py"
    ):
        module = determine_module.set_module(name, configuration)
        cluster = await module.request_cluster_data(
            session, url, layer, name, configuration
        )
        return cluster


"""RUNTIME"""


def generate_runtimes(configuration) -> list:
    from datetime import datetime, timedelta

    start = datetime.strptime(configuration["general"]["loop_init_time"], "%H:%M:%S")
    end = datetime.strptime(
        configuration["general"]["loop_init_time"], "%H:%M:%S"
    ) + timedelta(hours=24)
    return [
        (
            start
            + timedelta(
                hours=(configuration["general"]["loop_interval_minutes"]) * i / 60
            )
        ).strftime("%H:%M:%S")
        for i in range(
            int(
                (end - start).total_seconds()
                / 60.0
                / (configuration["general"]["loop_interval_minutes"])
            )
        )
    ]


def generate_rewards_runtimes() -> list:
    from datetime import datetime, timedelta

    start = datetime.strptime("12:00:00", "%H:%M:%S")
    end = datetime.strptime("12:00:00", "%H:%M:%S") + timedelta(hours=24)
    return [
        (start + timedelta(hours=5 * i / 60)).strftime("%H:%M:%S")
        for i in range(int((end - start).total_seconds() / 60.0 / 5))
    ]
