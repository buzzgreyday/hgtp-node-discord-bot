from functions import request


async def request_node_data(subscriber: dict, port: int, configuration: dict) -> tuple[dict, dict]:
    node_data, node_cluster_data = await request.node_cluster_data(subscriber, port, configuration)
    return node_data, node_cluster_data


async def merge_node_data(layer: int, latest_tessellation_version: str, node_data: dict, node_cluster_data: dict, configuration: dict) -> dict:
    lb_ids = []
    node_data["latestVersion"] = latest_tessellation_version
    node_data["layer"] = layer
    node_data["clusterPairs"] = [len(node_cluster_data)]
    node_data["clusterNames"] = []
    if node_cluster_data:
        lb_ids.extend(v for k, v in configuration["source ids"].items())
        for d in node_cluster_data:
            for k, v in d.items():
                if (k == "id") and (str(v) in lb_ids):
                    for node_cluster_name, node_id in configuration["source ids"].items():
                        if node_id == str(v):
                            node_data["clusterNames"].append(node_cluster_name.lower())
    return node_data
