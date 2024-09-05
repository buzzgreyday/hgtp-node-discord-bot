import asyncio
import logging
import os
import sys

import threading
import time
import traceback
from datetime import datetime
from typing import List, Tuple, Dict
import psutil

import hypercorn
import aiohttp
import yaml
from hypercorn.asyncio import serve
from hypercorn.config import Config

from assets.src import preliminaries, check, history, rewards, stats, api
from assets.src.config import configure_logging
from assets.src.database.database import update_user, optimize
from assets.src.discord import discord
from assets.src.discord.services import bot, discord_token

dev_env = os.getenv("NODEBOT_DEV_ENV")

MAX_CONCURRENT_REQUESTS = 10
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)


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





def timing():
    return datetime.now(), time.perf_counter()


"""MAIN LOOP"""


def start_rewards_coroutine(_configuration):
    asyncio.run_coroutine_threadsafe(rewards.run(_configuration), bot.loop)


def start_stats_coroutine(_configuration):
    asyncio.run_coroutine_threadsafe(stats.run(), bot.loop)


def start_database_optimization_coroutine(_configuration):
    asyncio.run_coroutine_threadsafe(optimize(_configuration), bot.loop)


def start_hypercorn_coroutine(app):
    asyncio.run_coroutine_threadsafe(run_hypercorn_process(app), bot.loop)




async def cache_and_clusters(session, cache, clusters, _configuration) -> Tuple[List[Dict], List[Dict]]:
    first_run = True if not cache else False
    if not clusters:
        # Since we need the clusters below, if they're not existing, create them
        for cluster_name, layers in _configuration["modules"].items():
            for layer in layers:
                clusters.append({"cluster_name": cluster_name, "layer": layer, "number_of_subs": 0})
        clusters.append({"cluster_name": None, "layer": 0, "number_of_subs": 0})
        clusters.append({"cluster_name": None, "layer": 1, "number_of_subs": 0})

    # Get subscribers to check with cache: this should follow same logic as above
    for layer in (0, 1):
        # We need subscriber data to determine if new subscriptions have been made (or first run), add these to cache
        layer_subscriptions = await api.get_user_ids(session, layer, None, _configuration)
        # Returns a list of tuples containing (ID, IP, PORT, REMOVAL_DATETIME, CLUSTER)

        for subscriber in layer_subscriptions:
            subscriber_found = False
            if not first_run:
                for cached_subscriber in cache:
                    # This will skip lookup if already located in a cluster. This needs reset every check.
                    cached_subscriber["located"] = False
                    if subscriber[0] == cached_subscriber["id"] and subscriber[1] == cached_subscriber["ip"] and subscriber[2] == cached_subscriber["public_port"]:
                        subscriber_found = True
                        if cached_subscriber["cluster_name"] in (None, 'None', False, 'False', '', [], {}, ()) and cached_subscriber["new_subscriber"] is True:
                            logging.getLogger("app").info(
                                f"main.py - Found new subscriber in cache\n"
                                f"Subscriber: ip {cached_subscriber["ip"]}, layer {cached_subscriber["public_port"]}\n"
                                f"Details: Unidentified cluster name"
                            )
                        else:
                            cached_subscriber["new_subscriber"] = False
                        break

                if not subscriber_found:
                    cache.append(
                        {
                            "id": subscriber[0],
                            "ip": subscriber[1],
                            "public_port": subscriber[2],
                            "layer": layer,
                            "cluster_name": None,
                            "located": False,
                            "new_subscriber": True,
                            "removal_datetime": subscriber[3]
                        }
                    )
                    logging.getLogger("app").info(
                        f"main.py - Found new subscriber in cache\n"
                        f"Subscriber: ip {subscriber[1]}, layer {subscriber[2]}\n"
                        f"Details: New subscriber added to cache"
                    )

            else:
                cache.append(
                        {
                            "id": subscriber[0],
                            "ip": subscriber[1],
                            "public_port": subscriber[2],
                            "layer": layer,
                            "cluster_name": subscriber[4],
                            "located": False,
                            "new_subscriber": False,
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
    return sorted_cache, sorted_clusters


async def main_loop(version_manager, _configuration):

    cache = []
    clusters = []
    while True:
        async with semaphore:
            async with aiohttp.ClientSession() as session:

                if hypercorn_running:
                    await bot.wait_until_ready()
                    try:
                        tasks = []
                        try:
                            cache, clusters = await cache_and_clusters(session, cache, clusters, _configuration)
                        except Exception:
                            logging.getLogger("app").error(
                                f"main.py - Unknown error - Cache or cluster error: {traceback.format_exc()}"
                            )
                            await asyncio.sleep(6)
                            continue

                        no_cluster_subscribers = []
                        for cluster in clusters:
                            dt_start, timer_start = timing()
                            if cluster["cluster_name"] not in (None, 'None', False, 'False', '', [], {}, ()):
                                # Need a check for if cluster is down, skip check
                                try:
                                    cluster_data = await preliminaries.supported_clusters(
                                        session, cluster["cluster_name"], cluster["layer"], _configuration
                                    )
                                    if not cluster_data:
                                        logging.getLogger("app").error(
                                        f"main.py - Unknown error - Get cluster data failed for {[cluster["cluster_name"], cluster["layer"]]}: {traceback.format_exc()}"
                                    )
                                except Exception:
                                    logging.getLogger("app").error(
                                        f"main.py - Unknown error - Get cluster data failed for {[cluster["cluster_name"], cluster["layer"]]}: {traceback.format_exc()}"
                                    )
                                    continue
                                for i, cached_subscriber in enumerate(cache):
                                    if cached_subscriber["located"] in (None, 'None', False, 'False', '', [], {}, ()):
                                        if cached_subscriber["cluster_name"] in (None, 'None', False, 'False', '', [], {}, ()) and cached_subscriber["new_subscriber"] is False:
                                            # We need to run these last
                                            no_cluster_subscribers.append(cached_subscriber)
                                        else:
                                            try:
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
                                            except Exception:
                                                logging.getLogger("app").error(
                                                    f"main.py - Unknown error - Check failed for {[cluster["cluster_name"], cluster["layer"]]}\n"
                                                    f"Subscriber: {cached_subscriber}\n"
                                                    f"Details: {traceback.format_exc()}"
                                                )
                                                continue

                                # Wait for all tasks to complete
                                results = await asyncio.gather(*[task for _, task in tasks])

                                # Handle the results
                                for (i, _), (data, updated_cache) in zip(tasks, results):
                                    if data:
                                        await history.write(data)
                                        await update_user(updated_cache)
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
                                dt_stop, timer_stop = timing()

                                logging.getLogger("app").info(
                                    f"main.py - main_loop\n"
                                    f"Cluster: {cluster["cluster_name"]} l{cluster["layer"]}\n"
                                    f"Automatic check: {round(timer_stop - timer_start, 2)} seconds"
                                )

                    except Exception as e:
                        try:
                            await discord.messages.send_traceback(bot, f"Bot experienced a critical error: {e}")
                            raise RuntimeError(f"Bot experienced a critical error: {traceback.format_exc()}")
                        except Exception as e:
                            logging.getLogger("app").error(
                                f"main.py - main_loop\n"
                                f"Error: Could not send traceback via Discord"
                            )
                            raise RuntimeError(f"Bot experienced a critical error: {e}")

                else:
                    # If uvicorn isn't running
                    logging.getLogger("app").error(
                        f"main.py - main_loop\n"
                        f"Error: Uvicorn isn't running"
                    )

        # After checks, give GIL something to do
        await asyncio.sleep(3)


hypercorn_running = False
async def run_hypercorn_process(app):
    global hypercorn_running
    hypercorn_running = True
    try:
        config = Config()
        config.bind = ["localhost:8000"]
        await serve(app, config)
    finally:
        hypercorn_running = False  # Ensure flag is reset on stop

def start_services(configuration, version_manager):
    from assets.src.database.database import app
    hypercorn_thread = threading.Thread(target=start_hypercorn_coroutine, args=(app,))
    get_tessellation_version_thread = threading.Thread(
        target=version_manager.update_version, daemon=True
    )
    rewards_thread = threading.Thread(target=start_rewards_coroutine, args=(configuration,))
    stats_thread = threading.Thread(
        target=start_stats_coroutine, args=(configuration,)
    )
    db_optimization_thread = threading.Thread(target=start_database_optimization_coroutine, args=(configuration,))

    get_tessellation_version_thread.start()
    hypercorn_thread.start()
    rewards_thread.start()
    stats_thread.start()
    db_optimization_thread.start()


async def run_bot(version_manager, configuration):
    if not dev_env:
        bot.load_extension("assets.src.discord.commands")
    bot.load_extension("assets.src.discord.events")

    # Start the main loop as a background task
    bot.loop.create_task(main_loop(version_manager, configuration))

    try:
        await bot.start(discord_token, reconnect=True)
    except Exception as e:
        logging.getLogger("bot").error(f"Bot encountered an exception: {str(e)}")
        await bot.close()


async def restart_bot(version_manager, configuration):
    while True:
        try:
            await run_bot(version_manager, configuration)
        except Exception as e:
            logging.getLogger("bot").error(f"Restarting bot after error: {str(e)}")
            await bot.close()
            time.sleep(5)  # Optional: delay before restarting
        else:
            break  # Exit the loop if the bot exits cleanly


def main():
    _configuration = load_configuration()

    configure_logging()

    version_manager = preliminaries.VersionManager(_configuration)

    # Start your threads (if necessary)
    start_services(_configuration, version_manager)

    # Run the bot with automatic restart on failure
    loop = asyncio.get_event_loop()
    loop.run_until_complete(restart_bot(version_manager, _configuration))


if __name__ == "__main__":
    if dev_env:
        print("Message: Development environment enabled!")
    main()
