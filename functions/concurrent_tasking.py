import logging
from datetime import datetime
import asyncio

import aiohttp

from functions import data_handling
import info


class Request:
    def __init__(self, url):
        self.url = url

    async def json(self):
        timeout = aiohttp.ClientTimeout(total=info.aiohttp_timeout)
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.debug(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST SUCCEEDED")
                else:
                    logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED")
            del resp
            await session.close()
        return data


async def request_node_data(subscriber, port):
    cluster_data = []

    if port is not None:
        try:
            node_data = await Request(f"http://{subscriber['ip']}:{port}/{info.node}").json()
        except Exception:
            node_data = {"state": "Offline", "session": None, "clusterSession": None, "version": None, "host": subscriber["ip"], "publicPort": port, "p2pPort": None, "id": None}
            logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE OFFLINE")

        for k, v in node_data.items():
            if (k == "state") and (v != "Offline"):
                try:
                    cluster_data = await Request(f"http://{subscriber['ip']}:{port}/{info.cluster}").json()
                    # cluster_data is a list of dictionaries
                except Exception:
                    pass
        # After this, check historic data
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
    historic_node_dataframe = await data_handling.nodes(info.latest_node_data)
    subscriber_dataframe = await data_handling.read_all_subscribers()
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
                    request_futures.append(asyncio.create_task(request_node_data(subscriber, port)))
            elif k == "public_l1":
                for port in v:
                    # create_task() here and append to futures
                    request_futures.append(asyncio.create_task(request_node_data(subscriber, port)))
        # return list of futures to main() and run there
    return request_futures
