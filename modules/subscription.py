import asyncio

from modules import read, locate


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
