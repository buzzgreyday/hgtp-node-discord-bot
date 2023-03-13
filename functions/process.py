async def node_cluster(node_data, cluster_data):
    if cluster_data:
        node_data["connections"] = len(cluster_data)
        print(node_data)
