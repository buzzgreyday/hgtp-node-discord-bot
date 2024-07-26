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
    except (FileNotFoundError, yaml.YAMLError) as e:
        logging.getLogger("app").error(f"main.py - Configuration file error: {e}")
        sys.exit(1)


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


def start_rewards_coroutine(_configuration):
    asyncio.run_coroutine_threadsafe(rewards.run(_configuration), bot.loop)


def start_stats_coroutine(_configuration):
    asyncio.run_coroutine_threadsafe(stats.run(), bot.loop)


async def cache_and_clusters(session, cache, clusters, _configuration) -> Tuple[List[Dict], List[Dict]]:

    if not clusters:
        for cluster_name, layers in _configuration["modules"].items():
            for layer in layers:
                clusters.append({"cluster_name": cluster_name, "layer": layer, "number_of_subs": 0})
        clusters.extend([
            {"cluster_name": None, "layer": 0, "number_of_subs": 0},
            {"cluster_name": None, "layer": 1, "number_of_subs": 0}
        ])

    # This should follow the above logic
    for layer in [0, 1]:
        layer_subscriptions = await api.get_user_ids(session, layer, None, _configuration)
        for subscriber in layer_subscriptions:
            if not any(subscriber[0] == cs["id"] and subscriber[1] == cs["ip"] and subscriber[2] == cs["public_port"]
                       for cs in cache):
                cache.append(
                    {
                        "id": subscriber[0],
                        "ip": subscriber[1],
                        "public_port": subscriber[2],
                        "layer": layer,
                        "cluster_name": f"{clusters[0]['cluster_name']}",
                        "located": False
                    }
                )

    for cluster in clusters:
        cluster["number_of_subs"] = sum(1 for cs in cache if cs["cluster_name"] == cluster["cluster_name"] and cs["layer"] == cluster["layer"])

    sorted_clusters = sorted(clusters, key=lambda k: k["number_of_subs"], reverse=True)
    priority_dict = {item['cluster_name']: int(item['number_of_subs']) for item in sorted_clusters}
    sorted_cache = sorted(cache, key=lambda x: priority_dict[x['cluster_name']], reverse=True)

    print(sorted_clusters)
    return sorted_cache, sorted_clusters


async def main_loop(version_manager, _configuration):
    times = preliminaries.generate_runtimes(_configuration)
    logging.getLogger("app").info(f"main.py - runtime schedule:\n\t{times}")

    cache = []
    clusters = []

    async def is_uvicorn_running():
        for process in psutil.process_iter(['pid', 'name']):
            try:
                if 'uvicorn' in process.name():
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        return False

    while True:
        async with aiohttp.ClientSession() as session:
            if await is_uvicorn_running():
                await bot.wait_until_ready()
                try:
                    tasks = []

                    current_time = datetime.now(timezone.utc).time().strftime("%H:%M:%S")
                    if current_time in times:
                        cache, clusters = await cache_and_clusters(session, cache, clusters, _configuration)
                        no_cluster_subscribers = []

                        for cluster in clusters:
                            cluster_name = cluster["cluster_name"]
                            if cluster_name:
                                dt_start, timer_start = dt.timing()
                                cluster_data = await preliminaries.supported_clusters(
                                    session, cluster_name, cluster["layer"], _configuration
                                )
                                print("Checking:", cluster_name, cluster["layer"])

                                for i, cached_subscriber in enumerate(cache):
                                    if not cached_subscriber["located"]:
                                        if not cached_subscriber["cluster_name"]:
                                            no_cluster_subscribers.append(cached_subscriber)
                                        else:
                                            task = asyncio.create_task(check.automatic(
                                                session,
                                                cached_subscriber,
                                                cluster_data,
                                                cluster_name,
                                                cluster["layer"],
                                                version_manager,
                                                _configuration
                                            ))
                                            tasks.append((i, task))

                                results = await asyncio.gather(*[task for _, task in tasks])

                                for (i, _), (data, updated_cache) in zip(tasks, results):
                                    await history.write(data)
                                    cache[i] = updated_cache

                                tasks.clear()

                                no_cluster_subscribers = list({tuple(cs.items()): cs for cs in no_cluster_subscribers}.values())

                                dt_stop, timer_stop = dt.timing()
                                print(
                                    f"main.py - L{cluster['layer']} {cluster_name}- Automatic check completed in "
                                    f"{round(timer_stop - timer_start, 2)} seconds"
                                )

                        print("Check these separately:", no_cluster_subscribers)

                    else:
                        await asyncio.sleep(0.2)

                except Exception as e:
                    logging.getLogger("app").error(f"main.py - error: {traceback.format_exc()}")
                    try:
                        await discord.messages.send_traceback(bot, traceback.format_exc())
                    except Exception:
                        logging.getLogger("app").error(f"main.py - Could not send traceback via Discord")


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
    configure_logging()

    version_manager = preliminaries.VersionManager(_configuration)

    bot.load_extension("assets.src.discord.events")

    bot.loop.create_task(main_loop(version_manager, _configuration))

    uvicorn_thread = threading.Thread(target=run_uvicorn_process)
    get_tessellation_version_thread = threading.Thread(
        target=version_manager.update_version, daemon=True
    )
    rewards_thread = threading.Thread(target=start_rewards_coroutine, args=(_configuration,))
    stats_thread = threading.Thread(target=start_stats_coroutine, args=(_configuration,))

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
