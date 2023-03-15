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
    print(historic_node_dataframe)
    return historic_node_dataframe


async def merge_history_data(node_data, historic_node_data):
    former_cluster_names = list(set(historic_node_data["cluster name"].values))
    for cluster_name in former_cluster_names:
        former_tessellation_version = historic_node_data["node version"][historic_node_data["cluster name"] == cluster_name].values[0]
        former_connectivity = historic_node_data["connectivity"][historic_node_data["cluster name"] == cluster_name].values[0]
