import time
import asyncio
from functions import read, request, latest_data, historic_data, clusters_data, new_data


async def preliminary_data(configuration):
    timer_start = time.perf_counter()
    tasks = []
    cluster_data = []
    validator_mainnet_data, validator_testnet_data = await request.validator_data(configuration)
    latest_tessellation_version = await request.latest_project_version_github(configuration)
    for cluster_layer, cluster_names in list(configuration["request"]["url"]["clusters"]["load balancer"].items()):
        tasks.append(asyncio.create_task(request.supported_clusters(cluster_layer, cluster_names, configuration)))
    for task in tasks:
        cluster_data.append(await task)
    timer_stop = time.perf_counter()
    print("PRELIMINARIES TOOK:", timer_stop - timer_start)
    return cluster_data, validator_mainnet_data, validator_testnet_data, latest_tessellation_version


async def create_per_subscriber_future(dask_client, subscriber: dict, layer: int, port: int, latest_tessellation_version: str,  validator_mainnet_data, validator_testnet_data, all_supported_clusters_data: list[dict], history_dataframe, configuration: dict) -> dict:
    node_data = await new_data.create(subscriber, port)
    node_data, node_cluster_data = await latest_data.request_node_data(subscriber, port, node_data, configuration)
    node_data = await latest_data.merge_node_data(layer, latest_tessellation_version, node_data, node_cluster_data, configuration)
    historic_node_dataframe = await historic_data.isolate_node_data(dask_client, node_data, history_dataframe)
    historic_node_dataframe = await historic_data.isolate_former_node_data(historic_node_dataframe)
    node_data = await historic_data.merge_node_data(node_data, historic_node_dataframe)
    node_data = await clusters_data.merge_node_data(node_data, validator_mainnet_data, validator_testnet_data, all_supported_clusters_data, configuration)
    node_data = await latest_data.request_wallet_data(node_data, configuration)
    # print(node_data)
    """REMEMBER TO CHECK DATE/TIME FOR LAST NOTICE"""
    # JUST SEE IF ID IS IN THE RETURNED DATA, DO NOT CHECK FOR CLUSTER NAME
    # REQUEST FROM HISTORIC DATA

    return node_data


async def registered_subscriber_node_data(dask_client, ip: str, subscriber_dataframe) -> dict:
    subscriber = {"name": await dask_client.compute(subscriber_dataframe.name[subscriber_dataframe.ip == ip]),
                  "contact": await dask_client.compute(subscriber_dataframe.contact[subscriber_dataframe.ip == ip]),
                  "ip": ip,
                  "public_l0": tuple(await dask_client.compute(subscriber_dataframe.public_l0[subscriber_dataframe.ip == ip])),
                  "public_l1": tuple(await dask_client.compute(subscriber_dataframe.public_l1[subscriber_dataframe.ip == ip]))}

    return subscriber


async def init(dask_client, latest_tessellation_version: str, validator_mainnet_data, validator_testnet_data, all_supported_cluster_data: list[dict], configuration: dict) -> list:
    subscriber_futures = []
    request_futures = []
    history_dataframe = await read.history(configuration)
    subscriber_dataframe = await read.subscribers(configuration)
    for ip in list(set(await dask_client.compute(subscriber_dataframe["ip"].values))):
        subscriber_futures.append(asyncio.create_task(registered_subscriber_node_data(dask_client, ip, subscriber_dataframe)))
    for _ in subscriber_futures:
        subscriber = await _
        for k, v in subscriber.items():
            if k == "public_l0":
                for port in v:
                    layer = 0
                    request_futures.append(asyncio.create_task(create_per_subscriber_future(dask_client, subscriber, layer, port, latest_tessellation_version, validator_mainnet_data, validator_testnet_data, all_supported_cluster_data, history_dataframe, configuration)))
            elif k == "public_l1":
                for port in v:
                    layer = 1
                    request_futures.append(asyncio.create_task(create_per_subscriber_future(dask_client, subscriber, layer, port, latest_tessellation_version, validator_mainnet_data, validator_testnet_data, all_supported_cluster_data, history_dataframe, configuration)))
    return request_futures
