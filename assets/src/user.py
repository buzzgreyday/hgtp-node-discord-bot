import asyncio
from typing import List

import pandas as pd

from assets.src import schemas, database, api, history, dt, cluster, determine_module

IP_REGEX = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"


async def node_status_check(subscriber, cluster_data: schemas.Cluster, version_manager, configuration: dict) -> schemas.Node:
    node_data = schemas.Node(name=subscriber.name.values[0],
                             contact=subscriber.contact.values[0],
                             ip=subscriber.ip.values[0],
                             layer=subscriber.layer.values[0],
                             public_port=subscriber.public_port.values[0],
                             id=subscriber.id.values[0],
                             wallet_address=subscriber.wallet.values[0],
                             latest_version=version_manager.get_version(),
                             notify=False,
                             timestamp_index=dt.datetime.utcnow())
    node_data = await history.node_data(None, node_data, configuration)
    found_in_cluster, cluster_data = cluster.locate_node(node_data, cluster_data)
    # last_known_cluster missing
    node_data = cluster.merge_data(node_data, found_in_cluster, cluster_data)
    node_data = await cluster.get_module_data(node_data, configuration)
    if node_data.cluster_name is not None and cluster_data is not None and configuration["modules"][node_data.cluster_name][node_data.layer]["rewards"]:
        node_data = determine_module.set_module(node_data.cluster_name, configuration).check_rewards(node_data, cluster_data)
    elif node_data.former_cluster_name is not None and cluster_data is not None and configuration["modules"][node_data.former_cluster_name][node_data.layer]["rewards"]:
        node_data = determine_module.set_module(node_data.former_cluster_name, configuration).check_rewards(node_data, cluster_data)
    elif node_data.last_known_cluster_name is not None and cluster_data is not None and configuration["modules"][node_data.last_known_cluster_name][node_data.layer]["rewards"]:
        node_data = determine_module.set_module(node_data.last_known_cluster_name, configuration).check_rewards(node_data, cluster_data)
    return node_data


async def process_node_data_per_user(name, ids, cluster_data, version_manager, _configuration) -> List[schemas.Node]:
    futures = []
    data = []
    if ids is not None:
        for lst in ids:
            id_ = lst[0]
            ip = lst[1]
            port = lst[2]
            while True:
                subscriber = await api.locate_node(_configuration, None, id_, ip, port)
                if subscriber:
                    break
            subscriber = pd.DataFrame(subscriber)
            futures.append(asyncio.create_task(
                node_status_check(subscriber, cluster_data, version_manager,
                                  _configuration)))
        for async_process in futures:
            d = await async_process

            if d.last_known_cluster_name == name:
                data.append(d)
        return data


async def write_db(data: List[schemas.User]):
    async with database.SessionLocal() as session:
        db = session
        for d in data:
            await database.post_user(data=d, db=db)
