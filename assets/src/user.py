import asyncio
from typing import List

import pandas as pd

from assets.src import schemas, database, node, api
from assets.src.discord.services import bot

IP_REGEX = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"


async def check(latest_tessellation_version, name, layer, requester, cluster_data, dt_start, process_msg, _configuration) -> List[schemas.Node]:
    futures = []
    data = []
    ids = await locate_ids(layer, requester, _configuration)
    if ids is not None:
        for lst in ids:
            id_ = lst[0]
            ip = lst[1]
            port = lst[2]
            subscriber = await locate_node(_configuration, requester, id_, ip, port)
            subscriber = pd.DataFrame(subscriber)

            futures.append(asyncio.create_task(
                node.check(bot, process_msg, requester, subscriber, port, layer,
                           latest_tessellation_version, cluster_data, dt_start,
                           _configuration)))
        for async_process in futures:
            d, process_msg = await async_process
            data.append(d)
        return data


async def locate_ids(layer, requester, _configuration):
    """NOT FUNCTIONING PROPERLY, HERE WE NEED A LIST/SET OF TUPLES CONTAINING ID, IP, PORT"""
    if requester is None:
        return await api.safe_request(f"http://127.0.0.1:8000/user/ids/{layer}", _configuration)
    else:
        return await api.Request(f"http://127.0.0.1:8000/user/ids/contact/{requester}").json(_configuration)


async def locate_node(_configuration, requester, id_, ip, port):
    """Locate every subscription where ID is id_
    return await dask_client.compute(subscriber_dataframe[subscriber_dataframe.id == id_])"""
    return await api.safe_request(f"http://127.0.0.1:8000/user/ids/{id_}/{ip}/{port}", _configuration)


async def write_db(data: List[schemas.User]):
    async with database.SessionLocal() as session:
        db = session
        for d in data:
            await database.post_user(data=d, db=db)
