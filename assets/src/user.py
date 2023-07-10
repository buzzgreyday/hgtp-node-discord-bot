import asyncio
import logging
import shutil
import sys
from datetime import datetime
from typing import List

from aiofiles import os
import pandas as pd


import dask.dataframe as dd

import assets.src.database
from assets.src import schemas, database, node, api
from assets.src.discord.services import bot

IP_REGEX = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"


async def check(dask_client, latest_tessellation_version, requester, history_dataframe, all_cluster_data, dt_start, process_msg, _configuration) -> List[
    schemas.Node]:
    futures = []
    data = []
    for id_ in await locate_ids(requester, _configuration):
        print("ID:", id_)
        subscriber = await locate_node(_configuration, id_)
        print("SUBSCRIBER NODE DATA",subscriber)
        for L in list(set(subscriber.layer)):
            for port in list(set(subscriber.public_port[subscriber.layer == L])):
                futures.append(asyncio.create_task(
                    node.check(dask_client, bot, process_msg, requester, subscriber, port, L,
                               latest_tessellation_version, history_dataframe, all_cluster_data, dt_start,
                               _configuration)))
    for async_process in futures:
        d, process_msg = await async_process
        data.append(d)
    return data


async def update_public_port(dask_client, node_data: schemas.Node):
    pass


async def locate_ids(requester, _configuration):
    print("REQUEST?", requester)
    if requester is None:
        ids = await api.safe_request("http://127.0.0.1:8000/user/ids", _configuration)
        print("IDS FROM LH API", ids)
        return ids
        # return list(set(await dask_client.compute(subscriber_dataframe["id"])))
    else:
        """return list(set(await dask_client.compute(
            subscriber_dataframe["id"][subscriber_dataframe["contact"].astype(dtype=int) == int(requester)])))"""
        return await api.Request(f"127.0.0.1:8000/user/node/contact/{requester}").json(_configuration)


async def locate_node(_configuration, id_):
    # Locate every subscription where ID is id_
    # return await dask_client.compute(subscriber_dataframe[subscriber_dataframe.id == id_])
    data = await api.safe_request(f"http://127.0.0.1:8000/user/ids/{id_}", _configuration)
    print(data)
    return data


async def write(dask_client, dataframe, configuration):
    dataframe = dataframe.repartition(npartitions=1)

    fut = dataframe.to_parquet(f'{configuration["file settings"]["locations"]["subscribers temp"]}',
                               overwrite=True, compute=False, write_index=False)
    await dask_client.compute(fut)
    if await os.path.exists(f'{configuration["file settings"]["locations"]["subscribers_new"]}'):
        shutil.rmtree(f'{configuration["file settings"]["locations"]["subscribers_new"]}')
    await os.rename(f'{configuration["file settings"]["locations"]["subscribers temp"]}', f'{configuration["file settings"]["locations"]["subscribers_new"]}')
    # Write the updated DataFrame to the temporary location


async def write_db(data: List[schemas.User]):
    async with database.SessionLocal() as session:
        db = session
        for d in data:
            await database.post_user(data=d, db=db)


async def read_db(configuration: dict):
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - GETTING SUBSCRIBER FROM DATABASE")


async def read(configuration: dict):
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - READING SUBSCRIBER DATA AND RETURNING DATAFRAME")
    if not await os.path.exists(configuration["file settings"]["locations"]["subscribers_new"]):
        print("NO USER DATABASE EXISTS")
        return dd.from_pandas(pd.DataFrame(columns=configuration["file settings"]["columns"]["subscribers_new"]), npartitions=1)
    elif await os.path.exists(configuration["file settings"]["locations"]["subscribers_new"]):
        print("USER DATABASE EXISTS")
        return dd.read_parquet(configuration["file settings"]["locations"]["subscribers_new"])
