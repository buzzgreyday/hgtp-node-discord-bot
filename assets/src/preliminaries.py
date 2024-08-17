import logging
import threading
import time
import traceback

import requests
from aiofiles import os
from os import getenv
from dotenv import load_dotenv

from assets.src import determine_module, schemas

load_dotenv()


GH_TOKEN = getenv("GH_TOKEN")

"""THREADS"""


class VersionManager:
    def __init__(self, configuration):
        self.lock = threading.Lock()
        self.configuration = configuration
        self.version = self._check_github_version()

    def update_version(self):
        while True:
            with self.lock:
                self.version = self._check_github_version()
            time.sleep(600)

    def _check_github_version(self):
        while True:
            try:
                data = requests.get(
                    f"https://api.github.com/repos/constellation-labs/tessellation/releases/latest",
                    headers={'Authorization': f'token {GH_TOKEN}'}
                ).json()
                version = data["tag_name"][1:]
                logging.getLogger("app").debug(
                    f"preliminaries.py - https://api.github.com/repos/constellation-labs/tessellation/releases/latest "
                    f"got version: {version}"
                )
                return version
            except (KeyError, requests.exceptions.ConnectionError):
                logging.getLogger("app").warning(
                    f"preliminaries.py - https://api.github.com/repos/constellation-labs/tessellation/releases/latest"
                    f"KeyError: "
                    f"forcing retry in {600} seconds\n\t{traceback.format_exc()}"
                )
                time.sleep(600)
            except Exception as e:
                logging.getLogger("app").error(
                    f"preliminaries.py - Error occurred while checking GitHub version: {e}"
                )
                raise

    def get_version(self):
        with self.lock:
            return self.version


async def supported_clusters(session, name: str, layer: int, configuration: dict) -> schemas.Cluster:
    url = configuration["modules"][name][layer]["url"][0]
    module_path = f"{configuration['file settings']['locations']['cluster modules']}/{name}.py"
    if await os.path.exists(module_path):
        module = determine_module.set_module(name, configuration)
        cluster = await module.request_cluster_data(session, url, layer, name, configuration)
        return cluster


def _generate_runtimes(configuration, interval_minutes) -> list:
    from datetime import datetime, timedelta

    start = datetime.strptime(configuration["general"]["loop_init_time"], "%H:%M:%S")
    end = start + timedelta(hours=24)
    return [
        (start + timedelta(hours=interval_minutes * i / 60)).strftime("%H:%M:%S")
        for i in range(int((end - start).total_seconds() / 60.0 / interval_minutes))
    ]


def generate_runtimes(configuration) -> list:
    return _generate_runtimes(configuration, configuration["general"]["loop_interval_minutes"])


def generate_rewards_runtimes() -> list:
    return _generate_runtimes({"general": {"loop_init_time": "12:00:00"}}, 5)


def generate_stats_runtimes() -> list:
    # daily = 60*24
    return _generate_runtimes({"general": {"loop_init_time": "02:59:59"}}, 1)
