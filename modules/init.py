import asyncio
from modules import read, request, merge, create, locate
from modules.temporaries import temporaries


async def check(dask_client, subscriber: dict, layer: int, port: int, latest_tessellation_version: str,  validator_mainnet_data, validator_testnet_data, all_supported_clusters_data: list[dict], history_dataframe, configuration: dict) -> dict:
    node_data = await create.snapshot(subscriber, port, layer, latest_tessellation_version)
    node_data = await locate.node(node_data, all_supported_clusters_data)
    historic_node_dataframe = await locate.historic_node_data(dask_client, node_data, history_dataframe)
    historic_node_dataframe = await locate.former_historic_node_data(historic_node_dataframe)
    node_data = await merge.historic_data(node_data, historic_node_dataframe)
    node_data = await merge.node_data(node_data, validator_mainnet_data, validator_testnet_data,
                                                       all_supported_clusters_data)
    node_data = await request.node_cluster(node_data, configuration)

    node_data = await temporaries.run(node_data, all_supported_clusters_data)
    # FINALLY WE NEED TO SORT PER SUBSCRIBER IN MAIN.PY SO WE CAN MATCH DATA
    print(node_data)
    """REMEMBER TO CHECK DATE/TIME FOR LAST NOTICE"""
    # JUST SEE IF ID IS IN THE RETURNED DATA, DO NOT CHECK FOR CLUSTER NAME
    # REQUEST FROM HISTORIC DATA

    return node_data


async def run(dask_client, latest_tessellation_version: str, validator_mainnet_data, validator_testnet_data, all_supported_cluster_data: list[dict], configuration: dict) -> list:
    subscriber_futures = []
    request_futures = []
    history_dataframe = await read.history(configuration)
    subscriber_dataframe = await read.subscribers(configuration)
    for ip in list(set(await dask_client.compute(subscriber_dataframe["ip"].values))):
        subscriber_futures.append(asyncio.create_task(locate.registered_subscriber_node_data(dask_client, ip, subscriber_dataframe)))
    for _ in subscriber_futures:
        subscriber = await _
        for k, v in subscriber.items():
            if k == "public_l0":
                for port in v:
                    layer = 0
                    request_futures.append(asyncio.create_task(check(dask_client, subscriber, layer, port, latest_tessellation_version, validator_mainnet_data, validator_testnet_data, all_supported_cluster_data, history_dataframe, configuration)))
            elif k == "public_l1":
                for port in v:
                    layer = 1
                    request_futures.append(asyncio.create_task(check(dask_client, subscriber, layer, port, latest_tessellation_version, validator_mainnet_data, validator_testnet_data, all_supported_cluster_data, history_dataframe, configuration)))

    return request_futures
