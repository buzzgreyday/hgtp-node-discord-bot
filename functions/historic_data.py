async def node_data(dask_client, node_data, history_dataframe):

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


async def former_data(node_data, historic_node_data):
    # STD/OFFLINE VALUES
    former_node_id = []
    former_node_connectivity = []
    former_node_wallet = []
    former_node_tessellation_version = []
    former_node_cluster_association_time = []
    former_node_cluster_dissociation_time = []
    former_node_total_disk_space = []
    former_node_free_disk_space = []

    former_node_data = historic_node_data[historic_node_data["index timestamp"] == historic_node_data["index timestamp"].max()]
    if not former_node_data.empty:
        former_cluster_names = list(set(former_node_data["cluster name"]))
        for cluster_name in former_cluster_names:
            former_node_id = former_node_data["node id"][former_node_data["cluster name"] == cluster_name].values[0]
            # former_cluster_name.append(former_node_data["cluster name"][former_node_data["cluster name"] == cluster_name].values[0])
            former_node_connectivity.append(former_node_data["connectivity"][former_node_data["cluster name"] == cluster_name].values[0])
            former_node_wallet.append(former_node_data["node wallet"][former_node_data["cluster name"] == cluster_name].values[0])
            former_node_tessellation_version.append(former_node_data["node version"][former_node_data["cluster name"] == cluster_name].values[0])
            former_node_cluster_association_time.append(former_node_data["association time"][former_node_data["cluster name"] == cluster_name].values[0])
            former_node_cluster_dissociation_time.append(former_node_data["dissociation time"][former_node_data["cluster name"] == cluster_name].values[0])
            former_node_total_disk_space.append(former_node_data["node total disk space"][former_node_data["cluster name"] == cluster_name].values[0])
            former_node_free_disk_space.append(former_node_data["node free disk space"][former_node_data["cluster name"] == cluster_name].values[0])
    if node_data["state"] == "Offline":
        node_data["id"] = former_node_id
