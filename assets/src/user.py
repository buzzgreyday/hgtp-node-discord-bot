import asyncio
import logging
import shutil
import sys
from datetime import datetime
from typing import List

from aiofiles import os
import pandas as pd


import dask.dataframe as dd

from assets.src import schemas, database, node, api, config
from assets.src.database import get_user_ids
from assets.src.discord.services import bot

IP_REGEX = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"


async def check(dask_client, latest_tessellation_version, requester, subscriber_dataframe, history_dataframe, all_cluster_data, dt_start, process_msg, _configuration) -> List[
    schemas.Node]:
    futures = []
    data = []
    for id_ in await locate_ids(dask_client, requester, subscriber_dataframe):
        print(id_)
        subscriber = await locate_node(dask_client, subscriber_dataframe, id_)
        for L in list(set(subscriber["layer"])):
            for port in list(set(subscriber.public_port[subscriber.layer == L])):
                futures.append(asyncio.create_task(
                    node.check(dask_client, bot, process_msg, requester, subscriber, port, L,
                               latest_tessellation_version, history_dataframe, all_cluster_data, dt_start,
                               _configuration)))
    for async_process in futures:
        try:
            d, process_msg = await async_process
            data.append(d)
        except Exception as e:
            logging.critical(repr(e.with_traceback(sys.exc_info())))
            exit(1)
    return data


async def update_public_port(dask_client, node_data: schemas.Node):
    pass


async def locate_ids(dask_client, requester, subscriber_dataframe):

    if requester is None:
        ids = await api.Request("128.0.0.1:8000/ids").json(await config.load())
        print(ids)
        return list(set(ids))
        # return list(set(await dask_client.compute(subscriber_dataframe["id"])))
    else:
        return list(set(await dask_client.compute(
            subscriber_dataframe["id"][subscriber_dataframe["contact"].astype(dtype=int) == int(requester)])))


async def locate_node(dask_client, subscriber_dataframe, id_):
    return await dask_client.compute(subscriber_dataframe[subscriber_dataframe.id == id_])


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
    for d in data:
        print(d)
        await database.create_user(d)


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
