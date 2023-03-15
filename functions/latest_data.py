async def merge_cluster_data(dask_client, layer, node_data, cluster_data, load_balancers):
    pair_ip = None
    pair_port = None
    node_data["nodeLayer"] = layer
    node_data["nodeClusterPairs"] = len(cluster_data)
    node_data["nodeClusterName"] = None
    node_data["nodeClusterIp"] = pair_ip
    node_data["nodeClusterPublicPort"] = pair_port
    if cluster_data:
        lb_ids = await dask_client.compute(load_balancers["id"].values)
        for d in cluster_data:
            for k, v in d.items():
                if (k == "id") and (str(v) in lb_ids):
                    node_cluster_name = await dask_client.compute(load_balancers["name"][load_balancers["id"] == str(v)])
                    node_data["nodeClusterName"] = node_cluster_name.values[0]
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