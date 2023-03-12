import logging
from datetime import datetime
import asyncio
from functions import data_handling
import info


async def subscriber_node_data(dask_client, ip, subscriber_dataframe, historic_node_dataframe):
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
    futures = []
    historic_node_dataframe = await data_handling.nodes(dask_client, info.latest_node_data)
    subscriber_dataframe = await data_handling.read_all_subscribers()
    ips = await dask_client.compute(subscriber_dataframe["ip"])
    # use set() to remove duplicates
    for i, ip in enumerate(list(set(ips.values))):
        futures.append(asyncio.create_task(subscriber_node_data(dask_client, ip, subscriber_dataframe, historic_node_dataframe)))
    for _ in futures:
        subscriber = await _
        for k, v in subscriber.items():
            if k == "public_l0":
                for port in v:
                    # create_task() here and append to futures
                    print(subscriber["name"], port)
            elif k == "public_l1":
                for port in v:
                    # create_task() here and append to futures
                    print(subscriber["name"], port)
        # run tasks here
