import os.path


async def request_supported_clusters(cluster_layer: str, cluster_names: dict, configuration: dict) -> list:
    import importlib.util
    import sys

    all_clusters_data = []
    for cluster_name, cluster_info in cluster_names.items():
        for lb_url in cluster_info["url"]:
            if os.path.exists(f"{configuration['file settings']['locations']['cluster functions']}/{cluster_name}.py"):
                spec = importlib.util.spec_from_file_location(f"{cluster_name}.init", f"{configuration['file settings']['locations']['cluster functions']}/{cluster_name}.py")
                module = importlib.util.module_from_spec(spec)
                sys.modules[f"{cluster_name}.init"] = module
                spec.loader.exec_module(module)
                cluster = await module.init(lb_url, cluster_layer, cluster_name, configuration)
                all_clusters_data.append(cluster)
    del lb_url, cluster_name, cluster_info
    return all_clusters_data


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
