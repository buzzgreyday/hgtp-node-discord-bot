async def isolate_node_data(dask_client, node_data: dict, history_dataframe):
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


async def isolate_former_node_data(historic_node_dataframe):
    return historic_node_dataframe[historic_node_dataframe["index timestamp"] == historic_node_dataframe["index timestamp"].max()]


async def merge_node_data(node_data: dict, historic_node_dataframe) -> dict:
    """IF HISTORIC DATA EXISTS"""
    if not historic_node_dataframe.empty:
        node_data["formerClusterNames"] = historic_node_dataframe["cluster name"].values[0]
        node_data["formerClusterConnectivity"] = historic_node_dataframe["connectivity"][historic_node_dataframe["cluster name"] == node_data["formerClusterNames"]].values[0]
        node_data["formerClusterAssociationTime"] = historic_node_dataframe["association time"][historic_node_dataframe["cluster name"] == node_data["formerClusterNames"]].values[0]
        node_data["formerClusterDissociationTime"] = historic_node_dataframe["dissociation time"][historic_node_dataframe["cluster name"] == node_data["formerClusterNames"]].values[0]
        if node_data["state"] == "Offline":
            node_data["id"] = str(historic_node_dataframe["node id"].values[0])
            node_data["nodeWalletAddress"] = str(historic_node_dataframe["node wallet"].values[0])
            node_data["version"] = str(historic_node_dataframe["node version"].values[0])
            node_data["diskSpaceTotal"] = float(historic_node_dataframe["node total disk space"])
            node_data["diskSpaceFree"] = float(historic_node_dataframe["node free disk space"])

    return node_data
