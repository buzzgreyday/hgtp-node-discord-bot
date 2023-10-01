import asyncio
import logging
from typing import List

import pandas as pd

from assets.src import dt, preliminaries, user, determine_module, api, history, schemas, cluster
from assets.src.discord import discord
from assets.src.discord.services import bot


async def automatic_check(cluster_name, layer, version_manager,  _configuration) -> List:
    logging.getLogger(__name__).info(f"main.py - Automatic {cluster_name, layer} check initiated")
    dt_start, timer_start = dt.timing()
    cluster_data = await preliminaries.supported_clusters(cluster_name, layer, _configuration)
    ids = await api.get_user_ids(layer, None, _configuration)

    await bot.wait_until_ready()
    data = await user.process_node_data_per_user(cluster_name, ids, cluster_data, version_manager, _configuration)
    data = await determine_module.notify(data, _configuration)
    logging.getLogger(__name__).info(f"discord.py - Handling {len(data), cluster_name} L{layer} nodes")
    await discord.send_notification(bot, data, _configuration)
    dt_stop, timer_stop = dt.timing()
    logging.getLogger(__name__).info(
        f"main.py - Automatic L{layer} check {cluster_name} completed in completed in {round(timer_stop - timer_start, 2)} seconds")
    return data


async def request_check(process_msg, layer, requester, _configuration):
    process_msg = await discord.update_request_process_msg(process_msg, 1, None)
    ids = await api.get_user_ids(layer, requester, _configuration)
    await bot.wait_until_ready()
    if ids is not None:
        version_manager = preliminaries.VersionManager(_configuration)
        for lst in ids:
            id_, ip, port = lst
            while True:
                subscriber = await api.locate_node(_configuration, requester, id_, ip, port)
                if subscriber:
                    break
                await asyncio.sleep(1)
            subscriber = pd.DataFrame(subscriber)
            node_data = schemas.Node(name=subscriber.name.values[0],
                                     contact=subscriber.contact.values[0],
                                     ip=subscriber.ip.values[0],
                                     layer=subscriber.layer.values[0],
                                     public_port=subscriber.public_port.values[0],
                                     id=subscriber.id.values[0],
                                     wallet_address=subscriber.wallet.values[0],
                                     latest_version=version_manager.get_version(),
                                     notify=True,
                                     timestamp_index=dt.datetime.utcnow())
            process_msg = await discord.update_request_process_msg(process_msg, 2, None)
            node_data = await history.node_data(requester, node_data, _configuration)
            process_msg = await discord.update_request_process_msg(process_msg, 3, f"{node_data.cluster_name} layer {node_data.layer}")
            node_data = await cluster.get_module_data(node_data, _configuration)
            process_msg = await discord.update_request_process_msg(process_msg, 5, None)
            await discord.send(bot, node_data, _configuration)
            await discord.update_request_process_msg(process_msg, 6, None)



