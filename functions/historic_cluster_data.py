async def get_node_data(dask_client, node_data, history_dataframe):

    # ISOLATE LAYER AND NODE IN HISTORIC DATA
    ip = None
    port = None
    for k, v in node_data.items():
        if k == "host":
            ip = v
        if k == "publicPort":
            port = v
    historic_node_dataframe = await dask_client.compute(history_dataframe[(history_dataframe["node ip"] == ip) & (history_dataframe["node port"] == port)])
    del ip, port, k, v
    return historic_node_dataframe


async def merge(node_data, historic_node_data):
    # STD/OFFLINE VALUES
    former_node_id = None
    former_node_cluster_connectivity = []
    former_node_wallet = None
    former_node_tessellation_version = None
    former_node_cluster_association_time = []
    former_node_cluster_dissociation_time = []
    former_node_total_disk_space = None
    former_node_free_disk_space = None
    former_node_data = historic_node_data[historic_node_data["index timestamp"] == historic_node_data["index timestamp"].max()]
    """IF HISTORIC DATA EXISTS"""
    if not former_node_data.empty:
        former_cluster_names = list(set(former_node_data["cluster name"]))
        for cluster_name in former_cluster_names:
            former_node_id = former_node_data["node id"][former_node_data["cluster name"] == cluster_name].values[0]
            former_node_cluster_connectivity.append(former_node_data["connectivity"][former_node_data["cluster name"] == cluster_name].values[0])
            former_node_wallet = former_node_data["node wallet"][former_node_data["cluster name"] == cluster_name].values[0]
            former_node_tessellation_version = former_node_data["node version"][former_node_data["cluster name"] == cluster_name].values[0]
            former_node_cluster_association_time.append(former_node_data["association time"][former_node_data["cluster name"] == cluster_name].values[0])
            former_node_cluster_dissociation_time.append(former_node_data["dissociation time"][former_node_data["cluster name"] == cluster_name].values[0])
            former_node_total_disk_space = former_node_data["node total disk space"][former_node_data["cluster name"] == cluster_name].values[0]
            former_node_free_disk_space = former_node_data["node free disk space"][former_node_data["cluster name"] == cluster_name].values[0]
        if node_data["state"] == "Offline":
            node_data["id"] = former_node_id
            node_data["nodeWalletAddress"] = former_node_wallet
            node_data["version"] = former_node_tessellation_version
            node_data["diskSpaceTotal"] = former_node_total_disk_space
            node_data["diskSpaceFree"] = former_node_free_disk_space
    node_data["formerClusterNames"] = former_cluster_names
    node_data["formerClusterConnectivity"] = former_node_cluster_connectivity
    node_data["formerClusterAssociationTime"] = former_node_cluster_association_time
    node_data["formerClusterDissociationTime"] = former_node_cluster_dissociation_time
    return node_data
