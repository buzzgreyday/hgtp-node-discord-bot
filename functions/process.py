from functions import format, read


async def node_cluster(dask_client, layer, node_data, cluster_data, load_balancers):
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
                if k == "ip":
                    pair_ip = str(v)
                    node_data["nodeClusterIp"] = pair_ip
                if k == "publicPort":
                    pair_port = str(v)
                    node_data["nodeClusterPublicPort"] = pair_port
    return node_data


async def historic_node_data(dask_client, node_data, historic_node_dataframe):
    # ISOLATE LAYER AND NODE IN HISTORIC DATA
    ip = None
    port = None
    for k, v in node_data.items():
        if k == "host":
            ip = v
        if k == "publicPort":
            port = v
    print(ip, port)
    historic_node_dataframe = await dask_client.compute(historic_node_dataframe[(historic_node_dataframe["node ip"] == ip) & (historic_node_dataframe["node port"] == port)])

    print(historic_node_dataframe)


