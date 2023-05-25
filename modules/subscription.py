import logging
import re
from datetime import datetime

import pandas as pd
from aiofiles import os

import dask.dataframe as dd

from modules import api

IP_REGEX = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"


async def update_public_port(dask_client, node_data):
    pass


async def locate_ids(dask_client, requester, subscriber_dataframe):
    if requester is None:
        return list(set(await dask_client.compute(subscriber_dataframe["id"])))
    else:
        return list(set(await dask_client.compute(
            subscriber_dataframe["id"][subscriber_dataframe["contact"].astype(dtype=int) == int(requester)])))


async def locate_node(dask_client, subscriber_dataframe, id_):
    return await dask_client.compute(subscriber_dataframe[subscriber_dataframe.id == id_])


async def write(dask_client, dataframe, configuration):
    fut = dataframe.to_parquet(f'{configuration["file settings"]["locations"]["subscribers_new"]}/temp',
                               append=False, overwrite=True, compute=False, write_index=False)
    await dask_client.compute(fut)
    await os.replace(f'{configuration["file settings"]["locations"]["subscribers_new"]}/temp/part.0.parquet',
                     f'{configuration["file settings"]["locations"]["subscribers_new"]}/part.0.parquet')


async def read(configuration: dict):
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - READING SUBSCRIBER DATA AND RETURNING DATAFRAME")
    if not await os.path.exists(configuration["file settings"]["locations"]["subscribers_new"]):
        return dd.from_pandas(pd.DataFrame(columns=configuration["file settings"]["columns"]["subscribers_new"]), npartitions=1)
    elif await os.path.exists(configuration["file settings"]["locations"]["subscribers_new"]):
        return dd.read_parquet(configuration["file settings"]["locations"]["subscribers_new"])


def slice_args_per_ip(args):
    sliced_args = []
    ips = list(set(filter(lambda ip: re.match(IP_REGEX, ip), args)))
    ip_idx = list(set(map(lambda ip: args.index(ip), ips)))
    for i in range(0, len(ip_idx)):
        if i + 1 < len(ip_idx):
            arg = args[ip_idx[i]: ip_idx[i + 1]]
            sliced_args.append(arg)
        else:
            arg = args[ip_idx[i]:]
            sliced_args.append(arg)
    return sliced_args


def clean_args(arg) -> tuple[list[int | None], list[int | None], str | None]:
    ip = None
    public_zero_ports = []
    public_one_ports = []

    for i, val in enumerate(arg):
        if re.match(IP_REGEX, val):
            ip = val
        elif val in ("z", "zero"):
            for port in arg[i + 1:]:
                if port.isdigit():
                    public_zero_ports.append(int(port))
                else:
                    break
        elif val in ("o", "one"):
            for port in arg[i + 1:]:
                if port.isdigit():
                    public_one_ports.append(int(port))
                else:
                    break
    return public_zero_ports, public_one_ports, ip


async def validate_subscriber(ip: str, port: str, configuration):
    print("Requesting:", f"http://{str(ip)}:{str(port)}/node/info")
    node_data = await request.safe(f"http://{ip}:{port}/{configuration['request']['url']['clusters']['url endings']['node info']}", configuration)
    return node_data["id"] if node_data is not None else None
