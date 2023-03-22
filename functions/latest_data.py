import aiofiles as aiofiles

from functions import request


async def request_node_data(subscriber: dict, port: int, node_data: dict, configuration: dict) -> tuple[dict, dict]:
    node_data, node_cluster_data = await request.node_cluster_data(subscriber, port, node_data, configuration)
    # ADD EVERY KEY MISSING HERE
    return node_data, node_cluster_data

async def request_supported_clusters(cluster_layer: int, cluster_names: dict, configuration: dict) -> list:

    async def locate_id_offline():
        return configuration["source ids"][cluster_layer][cluster_name]

    async def update_config_with_latest_values():
        import yaml
        for layer in configuration["source ids"]:
            if layer == data["layer"]:
                for cluster_name in configuration["source ids"][layer]:
                    if cluster_name == data["cluster name"]:
                        if configuration["source ids"][layer][cluster_name] != data["id"]:
                            configuration["source ids"][layer][cluster_name] = data["id"]
                            async with aiofiles.open("data/config.yml", "w") as file:
                                await file.write(yaml.dump(configuration))
        del layer


    all_clusters_data = []
    for cluster_name, cluster_info in cluster_names.items():
        for lb_url in cluster_info["url"]:
            cluster_resp = await request.cluster_data(f"{lb_url}/{configuration['request']['url']['clusters']['url endings']['cluster info']}", configuration)
            node_resp = await request.cluster_data(f"{lb_url}/{configuration['request']['url']['clusters']['url endings']['node info']}", configuration)
            if node_resp is None:
                cluster_state = "offline"
                cluster_id = await locate_id_offline()
                cluster_session = None
            else:
                cluster_state = str(node_resp['state']).lower()
                cluster_id = node_resp["id"]
                cluster_session = node_resp["clusterSession"]

            data = {
                "layer": cluster_layer,
                "cluster name": cluster_name,
                "state": cluster_state,
                "id": cluster_id,
                "pair count": len(cluster_resp),
                "clusterSession": cluster_session,
                # BELOW I CONVERT LIST OF NODE PAIRS TO A GENERATOR TO SAVE MEMORY
                "data": (node_pair for node_pair in cluster_resp)
            }
            await update_config_with_latest_values()
            all_clusters_data.append(data)
            cluster_resp.clear()
            del cluster_resp, node_resp, data
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
                    wallet_data = await request.wallet_data(f"{be_url}/addresses/{node_data['nodeWalletAddress']}/balance", configuration)
                    if wallet_data is not None:
                        node_data.update(await make_update(wallet_data))

        else:
            wallet_data = await request.wallet_data(f"{configuration['request']['url']['block explorer']['layer 0']['mainnet']}/addresses/{node_data['nodeWalletAddress']}/balance", configuration)
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
