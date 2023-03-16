async def merge_cluster_data(dask_client, layer, node_data, cluster_data, configuration):
    pair_ip = None
    pair_port = None
    lb_ids = []
    node_data["nodeLayer"] = layer
    node_data["nodeClusterPairs"] = len(cluster_data)
    node_data["nodeClusterName"] = None
    node_data["nodeClusterIp"] = pair_ip
    node_data["nodeClusterPublicPort"] = pair_port
    if cluster_data:
        lb_ids.extend(v for k, v in configuration["source ids"].items())
        for d in cluster_data:
            for k, v in d.items():
                if (k == "id") and (str(v) in lb_ids):
                    for node_cluster_name, node_id in configuration["source ids"].items():
                        if node_id == str(v):
                            node_data["nodeClusterName"] = node_cluster_name
                    del node_cluster_name, lb_ids
                if k == "ip":
                    pair_ip = str(v)
                    node_data["nodeClusterIp"] = pair_ip
                    del pair_ip
                if k == "publicPort":
                    pair_port = str(v)
                    node_data["nodeClusterPublicPort"] = pair_port
                    del pair_port
        cluster_data.clear()
        del lb_ids, cluster_data, k, v, d
    return node_data
