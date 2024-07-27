import asyncio
import logging
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import List, Tuple, Dict
import psutil

import aiohttp
import yaml
from aiohttp import ClientConnectorError

from assets.src import preliminaries, check, history, rewards, stats, api, dt
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


async def cache_and_clusters(session, cache, clusters, _configuration) -> Tuple[List[Dict], List[Dict]]:

    if not clusters:
        # Since we need the clusters below, if they're not existing, create them
        for cluster_name, layers in _configuration["modules"].items():
            for layer in layers:
                clusters.append({"cluster_name": cluster_name, "layer": layer, "number_of_subs": 0, "marked_for_removal": 0})
        clusters.append({"cluster_name": None, "layer": 0, "number_of_subs": 0})
        clusters.append({"cluster_name": None, "layer": 1, "number_of_subs": 0})

    # Get subscribers to check with cache: this should follow same logic as above
    for layer in [0, 1]:
        # We need subscriber data to determine if new subscriptions have been made (or first run), add these to cache
        layer_subscriptions = await api.get_user_ids(session, layer, None, _configuration)
        # Returns a list of tuples containing (ID, IP, PORT, REMOVAL_DATETIME)

        for subscriber in layer_subscriptions:
            subscriber_found = False
            if cache:
                for cached_subscriber in cache:
                    # This will skip lookup if already located in a cluster. This needs reset every check.
                    cached_subscriber["located"] = False
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
                        "cluster_name": "mainnet",
                        "located": False,
                        "removal_datetime": subscriber[3]
                    }
                )

    for cluster in clusters:
        cluster["number_of_subs"] = 0
        for cached_subscriber in cache:
            if cached_subscriber["cluster_name"]:
                if cached_subscriber["cluster_name"] == cluster["cluster_name"] and cached_subscriber["layer"] == cluster["layer"]:
                    cluster["number_of_subs"] += 1

    sorted_clusters = sorted(clusters, key=lambda k: k["number_of_subs"], reverse=True)
    priority_dict = {item['cluster_name']: int(item['number_of_subs']) for item in sorted_clusters}
    sorted_cache = sorted(cache, key=lambda x: priority_dict[x['cluster_name']], reverse=True)
    print(sorted_clusters)
    return sorted_cache, sorted_clusters


async def main_loop(version_manager, _configuration):
    times = preliminaries.generate_runtimes(_configuration)
    logging.getLogger("app").info(f"main.py - runtime schedule:\n\t{times}")

    cache = list()
    clusters = list()

    while True:
        async with aiohttp.ClientSession() as session:
            def is_uvicorn_running():
                for process in psutil.process_iter(['pid', 'name']):
                    try:
                        if 'uvicorn' in process.name():
                            return True
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        pass
                return False

            if is_uvicorn_running():
                await bot.wait_until_ready()
                try:
                    tasks = []
                    cache, clusters = await cache_and_clusters(session, cache, clusters, _configuration)
                    no_cluster_subscribers = []
                    for cluster in clusters:
                        dt_start, timer_start = dt.timing()
                        if cluster["cluster_name"] not in (None, 'None', False, 'False', '', [], {}, ()):
                            # Ned a check for if cluster is down, skip check
                            cluster_data = await preliminaries.supported_clusters(
                                session, cluster["cluster_name"], cluster["layer"], _configuration
                            )
                            print("Checking:", cluster["cluster_name"], cluster["layer"])
                            for i, cached_subscriber in enumerate(cache):
                                if cached_subscriber["located"] in (None, 'None', False, 'False', '', [], {}, ()):
                                    if cached_subscriber["cluster_name"] in (None, 'None', False, 'False', '', [], {}, ()):
                                        # We need to run these last
                                        no_cluster_subscribers.append(cached_subscriber)
                                    else:
                                        task = asyncio.create_task(check.automatic(
                                            session,
                                            cached_subscriber,
                                            cluster_data,
                                            cluster["cluster_name"],
                                            cluster["layer"],
                                            version_manager,
                                            _configuration
                                        ))
                                        tasks.append((i, task))

                            # Wait for all tasks to complete
                            results = await asyncio.gather(*[task for _, task in tasks])

                            # Handle the results
                            for (i, _), (data, updated_cache) in zip(tasks, results):
                                await history.write(data)
                                cache[i] = updated_cache  # Replace the old cache entry with the updated one

                            # Clear to make ready for next check
                            tasks.clear()

                            # These should be checked last: probably make sure these are not new subscribers
                            # (new subscribers are 'integrationnet' by default now, could be "new" or something)
                            # and then check once daily, until removal
                            tuple_of_tuples = [tuple(sorted(d.items())) for d in no_cluster_subscribers]

                            # Create a set to remove duplicates
                            unique_tuples = set(tuple_of_tuples)

                            # Convert tuples back to dictionaries
                            no_cluster_subscribers = [dict(t) for t in unique_tuples]

                            # Log the completion time
                            dt_stop, timer_stop = dt.timing()
                            print(
                                f"main.py - L{cluster["layer"]} {cluster["cluster_name"]}- Automatic check completed in completed in "
                                f"{round(timer_stop - timer_start, 2)} seconds"
                            )
                        print("Check these separately:", no_cluster_subscribers)


                    else:
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

def configure_logging():
    log_configs = [
        ("app", "assets/data/logs/app.log", logging.INFO),
        ("rewards", "assets/data/logs/rewards.log", logging.INFO),
        ("nextcord", "assets/data/logs/nextcord.log", logging.CRITICAL),
        ("stats", "assets/data/logs/stats.log", logging.DEBUG)
    ]

    for name, file, level in log_configs:
        logger = logging.getLogger(name)
        logger.setLevel(level)
        handler = logging.FileHandler(filename=file, encoding="utf-8", mode="w")
        handler.setFormatter(
            logging.Formatter("[%(asctime)s] %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)


def main():
    _configuration = load_configuration()

    configure_logging()

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
    rewards_thread = threading.Thread(target=start_rewards_coroutine, args=(_configuration,))
    stats_thread = threading.Thread(
        target=start_stats_coroutine, args=(_configuration,)
    )
    get_tessellation_version_thread.start()
    uvicorn_thread.start()
    rewards_thread.start()
    stats_thread.start()

    while True:
        try:
            bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
        except ClientConnectorError:
            time.sleep(6)


if __name__ == "__main__":
    main()
