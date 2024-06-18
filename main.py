import asyncio
import logging
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timezone

import aiohttp
import yaml
from aiohttp import ClientConnectorError

from assets.src import preliminaries, run_process, history, rewards, stats
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

    while True:
        async with asyncio.Semaphore(8):
            async with aiohttp.ClientSession() as session:
                try:
                    data_queue = asyncio.Queue()
                    tasks = []

                    current_time = datetime.now(timezone.utc).time().strftime("%H:%M:%S")
                    if current_time in times:
                        for cluster_name, layers in _configuration["modules"].items():
                            for layer in layers:
                                task = run_process.automatic_check(
                                    session,
                                    cluster_name,
                                    layer,
                                    version_manager,
                                    _configuration,
                                )
                                tasks.append(task)

                        for completed_task in asyncio.as_completed(tasks):
                            data = await completed_task
                            await data_queue.put(data)
                        while not data_queue.empty():
                            data = await data_queue.get()
                            await history.write(data)
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
    # get_tessellation_version_thread = threading.Thread(
    #    target=version_manager.update_version, daemon=True
    # )
    # get_tessellation_version_thread.start()
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
