from functions import request


async def request_node_data(subscriber: dict, port: int, node_data: dict, configuration: dict) -> tuple[dict, dict]:
    node_data, node_cluster_data = await request.node_cluster_data(subscriber, port, node_data, configuration)
    # ADD EVERY KEY MISSING HERE
    return node_data, node_cluster_data


async def request_wallet_data(node_data, configuration):

    async def make_update_data(data):
        return {
            "nodeWalletBalance": data["data"]["balance"]
        }

    for be_layer, be_names in configuration["request"]["url"]["block explorer"].items():
        if (node_data['clusterNames'] or node_data['formerClusterNames']) in list(be_names.keys()):
            for be_name, be_url in be_names.items():
                if be_name.lower() == (node_data['clusterNames'] or node_data['formerClusterNames']):
                    request_url = f"{be_url}/addresses/{node_data['nodeWalletAddress']}/balance"
                    wallet_data = await request.Request(request_url).json(configuration)
                    node_data.update(await make_update_data(wallet_data))

        else:
            request_url = f"{configuration['request']['url']['block explorer']['layer 0']['mainnet']}/addresses/{node_data['nodeWalletAddress']}/balance"
            wallet_data = await request.Request(request_url).json(configuration)
            node_data.update(await make_update_data(wallet_data))

    return node_data


async def merge_node_data(layer: int, latest_tessellation_version: str, node_data: dict, node_cluster_data: dict, configuration: dict) -> dict:
    lb_ids = []
    node_data["latestVersion"] = latest_tessellation_version
    node_data["layer"] = layer
    node_data["nodePairCount"] = len(node_cluster_data)
    if node_cluster_data:
        lb_ids.extend(v for v in configuration["source ids"].values())
        for d in node_cluster_data:
            for k, v in d.items():
                if (k == "id") and (str(v) in lb_ids):
                    for node_cluster_name, node_id in configuration["source ids"].items():
                        if node_id == str(v):
                            node_data["clusterNames"] = node_cluster_name.lower()
    return node_data
