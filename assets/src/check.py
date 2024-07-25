import logging

import pandas as pd

from assets.src import (
    dt,
    preliminaries,
    determine_module,
    api,
    history,
    schemas,
    cluster,
)
from assets.src.discord import discord
from assets.src.discord.services import bot
from assets.src.user import node_status_check


async def automatic(session, cached_subscriber, cluster_data, cluster_name, layer, version_manager, _configuration):
    logger = logging.getLogger("app")

    data = []

    cluster_found = False
    subscriber = await api.locate_node(session, _configuration, None, cached_subscriber["id"],
                                       cached_subscriber["ip"], cached_subscriber["public_port"])
    subscriber = pd.DataFrame(subscriber)
    node_data = await node_status_check(
        session,
        subscriber,
        cluster_data,
        version_manager,
        _configuration,
    )
    if node_data.last_known_cluster_name == cluster_name:
        cluster_found = True
        data.append(node_data)
        cached_subscriber["cluster_name"] = cluster_name
        cached_subscriber["located"] = True

    if not cluster_found:
        cached_subscriber["cluster_name"] = None

    data = await determine_module.notify(data, _configuration)

    logger.debug(
        f"run_process.py - Handling {len(data), cluster_name} L{layer} nodes"
    )
    # await discord.send_notification(bot, data, _configuration)

    return data, cached_subscriber


async def request(session, process_msg, layer, requester, _configuration):
    process_msg = await discord.update_request_process_msg(process_msg, 1, None)
    ids = await api.get_user_ids(session, layer, requester, _configuration)
    await bot.wait_until_ready()

    if ids:
        version_manager = preliminaries.VersionManager(_configuration)

        for lst in ids:
            id_, ip, port = lst[:3]

            while True:
                subscriber = await api.locate_node(
                    session, _configuration, requester, id_, ip, port
                )

                if subscriber:
                    break

            subscriber = pd.DataFrame(subscriber)
            node_data = schemas.Node(
                name=subscriber.name.values[0],
                contact=subscriber.discord.values[0],
                ip=subscriber.ip.values[0],
                layer=subscriber.layer.values[0],
                public_port=subscriber.public_port.values[0],
                id=subscriber.id.values[0],
                wallet_address=subscriber.wallet.values[0],
                latest_version=version_manager.get_version(),
                notify=True,
                timestamp_index=dt.datetime.utcnow(),
            )

            process_msg = await discord.update_request_process_msg(process_msg, 2, None)
            node_data = await history.node_data(session, requester, node_data, _configuration)
            process_msg = await discord.update_request_process_msg(
                process_msg, 3, f"{node_data.cluster_name} layer {node_data.layer}"
            )
            node_data = await cluster.get_module_data(session, node_data, _configuration)
            process_msg = await discord.update_request_process_msg(process_msg, 5, None)
            await discord.send(bot, node_data, _configuration)
            await discord.update_request_process_msg(process_msg, 6, None)
