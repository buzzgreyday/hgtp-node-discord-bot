import logging
import os
import datetime

import pandas as pd

from assets.src import (
    preliminaries,
    determine_module,
    api,
    history,
    schemas,
    cluster,
)
from assets.src.discord import discord, messages
from assets.src.discord.services import bot

dev_env = os.getenv("NODEBOT_DEV_ENV")


async def node_status(
        session,
        subscriber,
        cluster_data: schemas.Cluster,
        version_manager,
        configuration: dict,
) -> schemas.Node:
    node_data = schemas.Node(
        name=subscriber.name.values[0],
        discord=subscriber.discord.values[0],
        mail=subscriber.mail.values[0],
        phone=subscriber.phone.values[0],
        ip=subscriber.ip.values[0],
        layer=subscriber.layer.values[0],
        public_port=subscriber.public_port.values[0],
        id=subscriber.id.values[0],
        wallet_address=subscriber.wallet.values[0],
        latest_version=version_manager.get_version(),
        notify=False,
        timestamp_index=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
    )
    node_data = await history.node_data(session, None, node_data, configuration)
    found_in_cluster, cluster_data = cluster.locate_node(node_data, cluster_data)
    node_data = cluster.merge_data(node_data, found_in_cluster, cluster_data)
    node_data = await cluster.get_module_data(session, node_data, configuration)
    # You need to check rewards here, if association is made but cluster is down!
    # The way to do this is to check add addresses for cluster, even if cluster is down
    # Think I did this now
    if (
            node_data.cluster_name is not None
            and cluster_data is not None
            and configuration["modules"][node_data.cluster_name][node_data.layer]["rewards"]
    ):
        node_data = determine_module.set_module(
            node_data.cluster_name, configuration
        ).check_rewards(node_data, cluster_data)
    elif (
            node_data.former_cluster_name is not None
            and cluster_data is not None
            and configuration["modules"][node_data.former_cluster_name][node_data.layer][
                "rewards"
            ]
    ):
        node_data = determine_module.set_module(
            node_data.former_cluster_name, configuration
        ).check_rewards(node_data, cluster_data)
    elif (
            node_data.last_known_cluster_name is not None
            and cluster_data is not None
            and configuration["modules"][node_data.last_known_cluster_name][
                node_data.layer
            ]["rewards"]
    ):
        node_data = determine_module.set_module(
            node_data.last_known_cluster_name, configuration
        ).check_rewards(node_data, cluster_data)
    return node_data


async def automatic(session, cached_subscriber, cluster_data, cluster_name, layer, version_manager, _configuration):
    logger = logging.getLogger("app")

    data = []

    subscriber = await api.locate_node(session=session, node_id=cached_subscriber["id"],
                                       ip=cached_subscriber["ip"], port=cached_subscriber["public_port"])
    if subscriber:
        subscriber = pd.DataFrame(subscriber)
        node_data = await node_status(
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
                    session=session, node_id=id_, ip=ip, port=port
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
                timestamp_index=datetime.datetime.now(datetime.UTC).replace(tzinfo=None),
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
