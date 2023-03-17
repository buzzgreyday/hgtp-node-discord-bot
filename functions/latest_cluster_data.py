async def merge(layer, latest_tessellation_version, node_data, cluster_data, configuration):
    lb_ids = []
    node_data["latestVersion"] = latest_tessellation_version
    node_data["layer"] = layer
    node_data["clusterPairs"] = [len(cluster_data)]
    node_data["clusterName"] = []
    if cluster_data:
        lb_ids.extend(v for k, v in configuration["source ids"].items())
        for d in cluster_data:
            for k, v in d.items():
                if (k == "id") and (str(v) in lb_ids):
                    for node_cluster_name, node_id in configuration["source ids"].items():
                        if node_id == str(v):
                            node_data["clusterName"].append(node_cluster_name)
                    del node_cluster_name, lb_ids
        cluster_data.clear()
        del lb_ids, cluster_data, k, v, d
    return node_data
