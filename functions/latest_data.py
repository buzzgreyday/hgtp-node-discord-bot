import asyncio
import os.path
import sys
from os import getenv, path, makedirs
import aiofiles as aiofiles
from functions.clusters import mainnet
from functions import request

async def request_supported_clusters(cluster_layer: str, cluster_names: dict, configuration: dict) -> list:
    import importlib.util
    import sys

    async def locate_id_offline():
        return configuration["source ids"][cluster_layer][cluster_name]

    async def update_config_with_latest_values():
        import yaml
        for layer in configuration["source ids"]:
            if layer == cluster["layer"]:
                for cluster_name in configuration["source ids"][layer]:
                    if cluster_name == cluster["cluster name"]:
                        if configuration["source ids"][layer][cluster_name] != cluster["id"]:
                            configuration["source ids"][layer][cluster_name] = cluster["id"]
                            async with aiofiles.open("data/config.yml", "w") as file:
                                await file.write(yaml.dump(configuration))
        del layer

    all_clusters_data = []
    for cluster_name, cluster_info in cluster_names.items():
        for lb_url in cluster_info["url"]:
            if os.path.exists(f"{configuration['file settings']['locations']['cluster functions']}/{cluster_name}.py"):
                spec = importlib.util.spec_from_file_location("mainnet.cluster_data", f"{configuration['file settings']['locations']['cluster functions']}/{cluster_name}.py")
                foo = importlib.util.module_from_spec(spec)
                sys.modules["mainnet.cluster_data"] = foo
                spec.loader.exec_module(foo)
                cluster_resp = await foo.cluster_data(f"{lb_url}/{configuration['request']['url']['clusters']['url endings']['cluster info']}", configuration)
                node_resp = await foo.cluster_data(f"{lb_url}/{configuration['request']['url']['clusters']['url endings']['node info']}", configuration)
                latest_ordinal, latest_timestamp, addresses = await mainnet.locate_rewarded_addresses(cluster_layer, cluster_name, configuration)

                if node_resp is None:
                    cluster_state = "offline"; cluster_id = await locate_id_offline(); cluster_session = None
                else:
                    cluster_state = str(node_resp['state']).lower(); cluster_id = node_resp["id"]; cluster_session = node_resp["clusterSession"]

                cluster = {
                    "layer": cluster_layer,
                    "cluster name": cluster_name,
                    "state": cluster_state,
                    "id": cluster_id,
                    "pair count": len(cluster_resp),
                    "cluster session": cluster_session,
                    "latest ordinal": latest_ordinal,
                    "latest ordinal timestamp": latest_timestamp,
                    "recently rewarded": addresses
                    # "pair data": cluster_resp
                }
                await update_config_with_latest_values()
                all_clusters_data.append(cluster)
                cluster_resp.clear()
                del node_resp, cluster
    del lb_url, cluster_name, cluster_info
    return all_clusters_data


async def request_wallet_data(node_data, configuration):

    async def make_update(data):
        return {
            "nodeWalletBalance": data["data"]["balance"]
        }

    for be_layer, be_names in configuration["request"]["url"]["block explorer"].items():
        if (node_data['clusterNames'] or node_data['formerClusterNames']) in list(be_names.keys()):
            for be_name, be_url in be_names.items():
                if be_name.lower() == (node_data['clusterNames'] or node_data['formerClusterNames']):
                    wallet_data = await mainnet.wallet_data(f"{be_url}/addresses/{node_data['nodeWalletAddress']}/balance", configuration)
                    if wallet_data is not None:
                        node_data.update(await make_update(wallet_data))

        else:
            wallet_data = await mainnet.wallet_data(f"{configuration['request']['url']['block explorer']['layer 0']['mainnet']}/addresses/{node_data['nodeWalletAddress']}/balance", configuration)
            if wallet_data is not None:
                node_data.update(await make_update(wallet_data))

    return node_data


async def merge_node_data(layer: int, latest_tessellation_version: str, node_data: dict, node_cluster_data: dict, configuration: dict) -> dict:
    node_data["latestVersion"] = latest_tessellation_version
    node_data["layer"] = layer
    node_data["nodePairCount"] = len(node_cluster_data)
    for node_pair in node_cluster_data:
        if f"layer {layer}" in configuration["source ids"].keys():
            for cluster_layer in configuration["source ids"].keys():
                if cluster_layer == f"layer {layer}":
                    for cluster_name, cluster_id in configuration["source ids"][cluster_layer].items():
                        if cluster_id == node_pair["id"]:
                            node_data["clusterNames"] = cluster_name.lower()


    return node_data
