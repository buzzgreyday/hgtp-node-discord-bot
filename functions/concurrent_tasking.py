import logging
from datetime import datetime
import asyncio

from functions import read, request, process
import info


async def do_check(subscriber, port):
    historic_node_dataframe = await read.history(info.latest_node_data)
    node_data, cluster_data = await request.node_data(subscriber, port)
    await process.node_cluster(node_data, cluster_data)
    return node_data, cluster_data


async def subscriber_node_data(dask_client, ip, subscriber_dataframe):
    ip = ip
    name = await dask_client.compute(subscriber_dataframe.name[subscriber_dataframe.ip == ip])
    contact = await dask_client.compute(subscriber_dataframe.contact[subscriber_dataframe.ip == ip])
    public_l0 = tuple(await dask_client.compute(subscriber_dataframe.public_l0[subscriber_dataframe.ip == ip]))
    public_l1 = tuple(await dask_client.compute(subscriber_dataframe.public_l1[subscriber_dataframe.ip == ip]))
    subscriber = {"name": name.values[0], "contact": contact.values[0], "ip": ip, "public_l0": public_l0,
                  "public_l1": public_l1}
    logging.info(f'{datetime.utcnow().strftime("%H:%M:%S")} - CREATED SUBSCRIBER DICTIONARY FOR {name.values[0].upper()} {ip}, PORTS: L0{public_l0} L1{public_l1}')
    public_l0 = list(public_l0)
    public_l1 = list(public_l1)
    public_l0.clear()
    public_l1.clear()
    del ip, name, contact, public_l0, public_l1
    return subscriber


async def init(dask_client):
    subscriber_futures = []
    request_futures = []
    subscriber_dataframe = await read.subscribers()
    ips = await dask_client.compute(subscriber_dataframe["ip"])
    # use set() to remove duplicates
    for i, ip in enumerate(list(set(ips.values))):
        subscriber_futures.append(asyncio.create_task(subscriber_node_data(dask_client, ip, subscriber_dataframe)))
    for _ in subscriber_futures:
        subscriber = await _
        for k, v in subscriber.items():
            if k == "public_l0":
                for port in v:
                    # create_task() here and append to futures
                    request_futures.append(asyncio.create_task(do_check(subscriber, port)))
            elif k == "public_l1":
                for port in v:
                    # create_task() here and append to futures
                    request_futures.append(asyncio.create_task(do_check(subscriber, port)))
        # return list of futures to main() and run there
    return request_futures
