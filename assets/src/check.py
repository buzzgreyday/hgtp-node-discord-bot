import datetime
import logging
import os

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
from assets.src.discord import discord, messages
from assets.src.discord.services import bot
from assets.src.user import node_status_check

dev_env = os.getenv("NODEBOT_DEV_ENV")

async def automatic(session, cached_subscriber, cluster_data, cluster_name, layer, version_manager, _configuration):
    logger = logging.getLogger("app")

    data = []

    subscriber = await api.locate_node(session, _configuration, None, cached_subscriber["id"],
                                       cached_subscriber["ip"], cached_subscriber["public_port"])
    if subscriber:
        subscriber = pd.DataFrame(subscriber)
        node_data = await node_status_check(
            session,
            subscriber,
            cluster_data,
            version_manager,
            _configuration,
        )
        # We might need to add a last_located time to database
        if node_data.last_known_cluster_name == cluster_name and node_data.layer == layer:
            data.append(node_data)
            cached_subscriber["cluster_name"] = cluster_name
            cached_subscriber["located"] = True
            cached_subscriber["removal_datetime"] = None

        if not node_data.last_known_cluster_name and node_data.layer == layer:
            cached_subscriber["cluster_name"] = None
            if cached_subscriber["removal_datetime"] in (None, 'None'):
                cached_subscriber["removal_datetime"] = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=30)

        data = await determine_module.notify(data, _configuration)

        logger.debug(
            f"run_process.py - Handling {len(data), cluster_name} L{layer} nodes"
        )
        await discord.send_notification(bot, data, _configuration)

        return data, cached_subscriber
    else:
        logger.warning(
            f"check.py - error - Subscriber is empty.\n"
            f"Subscriber: {cached_subscriber}"
        )
        return None, cached_subscriber


async def request(session, process_msg, layer, requester, _configuration):
    process_msg = await messages.update_request_process_msg(process_msg, 1)
    ids = await api.get_user_ids(session, layer, requester, _configuration)
    await bot.wait_until_ready()

    if ids:
        version_manager = preliminaries.VersionManager(_configuration)
        process_msg = await messages.update_request_process_msg(process_msg, 2)
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
                timestamp_index=dt.datetime.now(datetime.UTC),
            )

            process_msg = await messages.update_request_process_msg(process_msg, 3)
            node_data = await history.node_data(session, requester, node_data, _configuration)
            process_msg = await messages.update_request_process_msg(
                process_msg, 4
            )
            node_data = await cluster.get_module_data(session, node_data, _configuration)
            process_msg = await messages.update_request_process_msg(process_msg, 5)
            await discord.send(bot, node_data, _configuration)
            await messages.update_request_process_msg(process_msg, 6)
