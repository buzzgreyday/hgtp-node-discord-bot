import asyncio
import logging
import sys
import threading
import traceback
from datetime import datetime

import yaml

from assets.src import preliminaries, run_process, history
from assets.src.discord import discord
from assets.src.discord.services import bot, discord_token


def load_configuration():
    try:
        with open("config.yml", "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.getLogger(__name__).info(
            f"main.py - Configuration file 'config.yml' not found"
        )
        sys.exit(1)
    except yaml.YAMLError:
        logging.getLogger(__name__).info(
            f"main.py - Invalid configuration file 'config.yml'"
        )
        sys.exit(1)


"""MAIN LOOP"""


async def main_loop(version_manager, _configuration):
    times = preliminaries.generate_runtimes(_configuration)
    logging.getLogger(__name__).info(f"main.py - runtime schedule:\n\t{times}")
    while True:
        try:
            data_queue = asyncio.Queue()
            tasks = []
            if datetime.time(datetime.utcnow()).strftime("%H:%M:%S") in times:
                for cluster_name in _configuration["modules"].keys():
                    for layer in _configuration["modules"][cluster_name].keys():
                        task = run_process.automatic_check(
                            cluster_name, layer, version_manager, _configuration
                        )
                        tasks.append(task)
                for completed_task in asyncio.as_completed(tasks):
                    data = await completed_task
                    await data_queue.put(data)
                while not data_queue.empty():
                    data = await data_queue.get()
                    await history.write(data)
        except Exception:
            logging.getLogger(__name__).error(
                f"main.py - error: {traceback.format_exc()}\n\tCurrent check exited..."
            )
            await discord.messages.send_traceback(bot, traceback.format_exc())
        finally:
            await asyncio.sleep(1)


def main():
    _configuration = load_configuration()

    logging.basicConfig(
        filename=_configuration["file settings"]["locations"]["log"],
        filemode="w",
        format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
        level=logging.ERROR,
    )

    version_manager = preliminaries.VersionManager(_configuration)

    bot.load_extension("assets.src.discord.commands")
    bot.load_extension("assets.src.discord.events")

    bot.loop.create_task(main_loop(version_manager, _configuration))

    # Create a thread for running uvicorn
    uvicorn_thread = threading.Thread(target=preliminaries.run_uvicorn)
    get_tessellation_version_thread = threading.Thread(
        target=version_manager.update_version
    )
    get_tessellation_version_thread.daemon = True
    get_tessellation_version_thread.start()
    uvicorn_thread.start()
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))


if __name__ == "__main__":
    main()
