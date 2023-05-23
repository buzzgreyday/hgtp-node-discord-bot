import logging
from datetime import datetime

import pandas as pd
from aiofiles import os

import dask.dataframe as dd


async def update_public_port(dask_client, node_data):
    pass


async def locate_ids(dask_client, requester, subscriber_dataframe):
    if requester is None:
        return list(set(await dask_client.compute(subscriber_dataframe["id"])))
    else:
        return list(set(await dask_client.compute(
            subscriber_dataframe["id"][subscriber_dataframe["contact"].astype(dtype=int) == int(requester)])))


async def locate_node(dask_client, subscriber_dataframe, node_id):
    return await dask_client.compute(subscriber_dataframe[subscriber_dataframe.id == node_id])


async def write(dask_client, dataframe, configuration):
    dataframe.to_parquet(configuration["file settings"]["locations"]["subscribers_new"], overwrite=True, write_index=False).compute()


async def read(configuration: dict):
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - READING SUBSCRIBER DATA AND RETURNING DATAFRAME")
    if not await os.path.exists(configuration["file settings"]["locations"]["subscribers_new"]):
        df = pd.DataFrame(columns=configuration["file settings"]["columns"]["subscribers_new"])
        return dd.from_pandas(df, npartitions=1)
    elif await os.path.exists(configuration["file settings"]["locations"]["subscribers_new"]):
        return dd.read_parquet(configuration["file settings"]["locations"]["subscribers_new"], columns=configuration["file settings"]["columns"]["subscribers_new"])

