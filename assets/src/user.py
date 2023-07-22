import asyncio
from typing import List

import pandas as pd

from assets.src import schemas, database, node, api
from assets.src.discord.services import bot

IP_REGEX = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"


async def check(latest_tessellation_version, requester, all_cluster_data, dt_start, process_msg, _configuration) -> List[
    schemas.Node]:
    futures = []
    data = []
    for id_ in await locate_ids(requester, _configuration):
        subscriber = await locate_node(_configuration, id_)
        subscriber = pd.DataFrame(subscriber)
        for L in list(set(subscriber.layer)):
            for port in list(set(subscriber.public_port[subscriber.layer == L])):
                futures.append(asyncio.create_task(
                    node.check(bot, process_msg, requester, subscriber, port, L,
                               latest_tessellation_version, all_cluster_data, dt_start,
                               _configuration)))
    for async_process in futures:
        d, process_msg = await async_process
        data.append(d)
    return data


async def locate_ids(requester, _configuration):
    if requester is None:
        ids = await api.safe_request("http://127.0.0.1:8000/user/ids", _configuration)
        return ids
    else:
        return await api.Request(f"http://127.0.0.1:8000/user/ids/contact/{requester}").json(_configuration)


async def locate_node(_configuration, id_):
    """Locate every subscription where ID is id_
    return await dask_client.compute(subscriber_dataframe[subscriber_dataframe.id == id_])"""
    data = await api.safe_request(f"http://127.0.0.1:8000/user/ids/{id_}", _configuration)
    return data


async def write_db(data: List[schemas.User]):
    async with database.SessionLocal() as session:
        db = session
        for d in data:
            await database.post_user(data=d, db=db)
