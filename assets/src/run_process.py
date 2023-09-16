import asyncio
import logging

import pandas as pd

from assets.src import dt, preliminaries, user, determine_module, api, history, schemas, cluster
from assets.src.discord import discord
from assets.src.discord.services import bot
from main import version_manager


async def main(ctx, process_msg, requester, cluster_name, layer, _configuration) -> None:
    if requester is None:
        logging.getLogger(__name__).info(f"main.py - Automatic {cluster_name, layer} check initiated")
    else:
        logging.getLogger(__name__).info(f"main.py - Request from {requester} initiated")
    # GET GITHUB VERSION HERE
    dt_start, timer_start = dt.timing()
    process_msg = await discord.update_request_process_msg(process_msg, 1, None)
    cluster_data = await preliminaries.supported_clusters(cluster_name, layer, _configuration)
    ids = await api.get_user_ids(layer, requester, _configuration)

    await bot.wait_until_ready()
    data = await user.process_node_data_per_user(cluster_name, ids, requester, cluster_data, process_msg, version_manager, _configuration)
    process_msg = await discord.update_request_process_msg(process_msg, 5, None)
    data = await determine_module.notify(data, _configuration)
    process_msg = await discord.update_request_process_msg(process_msg, 6, None)
    if not process_msg:
        await history.write(data)
    await discord.send(ctx, process_msg, bot, data, _configuration)
    await discord.update_request_process_msg(process_msg, 7, None)
    dt_stop, timer_stop = dt.timing()
    if requester is None:
        logging.getLogger(__name__).info(
            f"main.py - Automatic {cluster_name, layer} check completed in {round(timer_stop - timer_start, 2)} seconds")
    else:
        logging.getLogger(__name__).info(
            f"main.py - Request from {requester} completed in {round(timer_stop - timer_start, 2)} seconds")


async def request_report(ctx, process_msg, layer, requester, _configuration):
    process_msg = await discord.update_request_process_msg(process_msg, 1, None)
    ids = await api.get_user_ids(layer, requester, _configuration)
    await bot.wait_until_ready()
    if ids is not None:
        for lst in ids:
            id_ = lst[0]
            ip = lst[1]
            port = lst[2]
            while True:
                subscriber = await api.locate_node(_configuration, requester, id_, ip, port)
                if subscriber:
                    break
            subscriber = pd.DataFrame(subscriber)
            node_data = schemas.Node(name=subscriber.name.values[0],
                                     contact=subscriber.contact.values[0],
                                     ip=subscriber.ip.values[0],
                                     layer=subscriber.layer.values[0],
                                     public_port=subscriber.public_port.values[0],
                                     id=subscriber.id.values[0],
                                     wallet_address=subscriber.wallet.values[0],
                                     latest_version=version_manager.get_version(),
                                     notify=False if requester is None else True,
                                     timestamp_index=dt.datetime.utcnow())
            node_data = await history.node_data(requester, node_data, _configuration)
            process_msg = await discord.update_request_process_msg(process_msg, 3, None)
            node_data, process_msg = await cluster.get_module_data(process_msg, node_data, _configuration)
            process_msg = await discord.update_request_process_msg(process_msg, 5, None)
            process_msg = await discord.update_request_process_msg(process_msg, 6, None)
            await discord.request_send(ctx, process_msg, bot, node_data, _configuration)
            await discord.update_request_process_msg(process_msg, 7, None)



