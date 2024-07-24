import asyncio
import logging
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import List, Tuple, Dict

import aiohttp
import yaml
from aiohttp import ClientConnectorError

from assets.src import preliminaries, run_process, history, rewards, stats, api
from assets.src.discord import discord
from assets.src.discord.services import bot, discord_token


def load_configuration():
    try:
        with open("config.yml", "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.getLogger("app").error(
            f"main.py - Configuration file 'config.yml' not found"
        )
        sys.exit(1)
    except yaml.YAMLError:
        logging.getLogger("app").error(
            f"main.py - Invalid configuration file 'config.yml'"
        )
        sys.exit(1)


"""MAIN LOOP"""


def start_rewards_coroutine(_configuration):
    asyncio.run_coroutine_threadsafe(rewards.run(_configuration), bot.loop)


def start_stats_coroutine(_configuration):
    asyncio.run_coroutine_threadsafe(stats.run(), bot.loop)


async def main_loop(version_manager, _configuration):
    times = preliminaries.generate_runtimes(_configuration)
    logging.getLogger("app").info(f"main.py - runtime schedule:\n\t{times}")

    cache = list()
    clusters = list()

    async def cache_and_clusters(session, cache, clusters) -> Tuple[List[Dict], List[Dict]]:

        if not clusters:
            # Since we need the clusters below, if they're not existing, create them
            for cluster_name, layers in _configuration["modules"].items():
                for layer in layers:
                    clusters.append({"cluster_name": cluster_name, "layer": layer, "number_of_subs": 0})
        print(clusters)

        """Get subscribers to check with cache"""
        for layer in range(0,1):
            # We need subscriber data to determine if new subscriptions have been made (or first run), add these to cache
            layer_subscriptions = await api.get_user_ids(session, layer, None, _configuration)
            # Returns a list of tuples containing (ID, IP, PORT)

            for subscriber in layer_subscriptions:
                subscriber_found = False
                if cache:
                    for cached_subscriber in cache:
                        if subscriber[0] == cached_subscriber["id"] and subscriber[1] == cached_subscriber["ip"] and subscriber[2] == cached_subscriber["public_port"]:
                            subscriber_found = True
                            break

                if not subscriber_found:
                    cache.append(
                        {
                            "id": subscriber[0],
                            "ip": subscriber[1],
                            "public_port": subscriber[2],
                            "layer": layer,
                            # Choose the most likely subscription by default, if new subscription or first run
                            "cluster_name": f"{clusters[0]["cluster_name"]}"
                        }
                    )
        for cached_subscriber in cache:
            for cluster in clusters:
                cluster["number_of_subs"] = 0
                if cached_subscriber["cluster_name"] == cluster["cluster_name"] and cached_subscriber["layer"] == cluster["layer"]:
                    cluster["number_of_subs"] += 1
        clusters_new = sorted(clusters, key=lambda i: i["number_of_subs"])
        clusters = clusters_new

        del clusters_new

        # If cache is found, then reorder/check hierarchy, while taking note of how many IPs in respective clusters
        print(cache, clusters)

        return cache, clusters


    while True:
        async with aiohttp.ClientSession() as session:
            try:
                tasks = []

                current_time = datetime.now(timezone.utc).time().strftime("%H:%M:%S")
                # if current_time in times:
                cache, clusters = await cache_and_clusters(session, cache, clusters)
                for cluster in clusters:
                    tasks.append(run_process.automatic_check(
                        session,
                        cache,
                        cluster["cluster_name"],
                        cluster["layer"],
                        version_manager,
                        _configuration,
                    )
                    )
                data, cache = await asyncio.gather(*tasks)
                print(data)
                await history.write(data)

                #else:
                await asyncio.sleep(0.2)

            except Exception as e:
                logging.getLogger("app").error(
                    f"main.py - error: {traceback.format_exc()}"
                )
                try:
                    await discord.messages.send_traceback(bot, traceback.format_exc())
                except Exception:
                    logging.getLogger("app").error(
                        f"main.py - Could not send traceback via Discord"
                    )


def run_uvicorn_process():
    subprocess.run(
        [
            "venv/bin/uvicorn",
            "assets.src.database.database:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8000",
            "--log-config",
            "assets/data/logs/bot/uvicorn.ini",
        ]
    )


def main():
    _configuration = load_configuration()

    logger = logging.getLogger("app")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(
        filename="assets/data/logs/app.log", encoding="utf-8", mode="w"
    )
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)

    logger = logging.getLogger("rewards")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(
        filename="assets/data/logs/rewards.log", encoding="utf-8", mode="w"
    )
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)

    logger = logging.getLogger("nextcord")
    logger.setLevel(logging.CRITICAL)
    handler = logging.FileHandler(
        filename="assets/data/logs/nextcord.log", encoding="utf-8", mode="w"
    )
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)

    logger = logging.getLogger("stats")
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(
        filename="assets/data/logs/stats.log", encoding="utf-8", mode="w"
    )
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)

    version_manager = preliminaries.VersionManager(_configuration)

    # bot.load_extension("assets.src.discord.commands")
    bot.load_extension("assets.src.discord.events")

    bot.loop.create_task(main_loop(version_manager, _configuration))

    # Create a thread for running uvicorn
    uvicorn_thread = threading.Thread(target=run_uvicorn_process)
    # Create a thread for running version check
    get_tessellation_version_thread = threading.Thread(
        target=version_manager.update_version, daemon=True
    )
    get_tessellation_version_thread.start()
    uvicorn_thread.start()
    rewards_thread = threading.Thread(target=start_rewards_coroutine, args=(_configuration,))
    rewards_thread.start()
    stats_thread = threading.Thread(
        target=start_stats_coroutine, args=(_configuration,)
    )
    stats_thread.start()

    while True:
        try:
            bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
        except ClientConnectorError:
            time.sleep(6)


if __name__ == "__main__":
    main()
