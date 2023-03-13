from functions import format, read


async def online_node_cluster(dask_client, node_data, cluster_data):
    pair_ip = None
    pair_port = None
    node_data["nodClusterPairs"] = len(cluster_data)
    node_data["nodeClusterName"] = None
    node_data["nodeClusterIp"] = pair_ip
    node_data["nodeClusterPublicPort"] = pair_port
    load_balancers = await read.load_balancers()
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

