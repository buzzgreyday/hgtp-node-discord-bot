async def historic_data(node_data: dict, historic_node_dataframe) -> dict:
    class Clean:
        def __init__(self, value):
            self._value = value

        def make_lower(self) -> str | None:
            self._value = Clean(self._value).make_none()
            if self._value is not None:
                return self._value.lower()
        def make_none(self) -> str | None:
            if len(self._value) != 0:
                return self._value.values[0]
            else:
                return None


    """IF HISTORIC DATA EXISTS"""
    if not historic_node_dataframe.empty:
        # node_data["formerClusterNames"] = str(historic_node_dataframe["cluster name"].values[0])
        node_data["formerClusterNames"] = Clean(historic_node_dataframe["cluster name"]).make_lower()
        node_data["formerClusterConnectivity"] = Clean(historic_node_dataframe["connectivity"][historic_node_dataframe["cluster name"] == node_data["formerClusterNames"]]).make_none()
        node_data["formerClusterAssociationTime"] = Clean(historic_node_dataframe["association time"][historic_node_dataframe["cluster name"] == node_data["formerClusterNames"]]).make_none()
        node_data["formerClusterDissociationTime"] = Clean(historic_node_dataframe["dissociation time"][historic_node_dataframe["cluster name"] == node_data["formerClusterNames"]]).make_none()
        if node_data["state"] == "Offline":
            node_data["id"] = Clean(historic_node_dataframe["node id"]).make_none()
            node_data["nodeWalletAddress"] = Clean(historic_node_dataframe["node wallet"]).make_none()
            node_data["version"] = Clean(historic_node_dataframe["node version"]).make_none()
            node_data["cpuCount"] = float(historic_node_dataframe["node cpu count"])
            node_data["diskSpaceTotal"] = float(historic_node_dataframe["node total disk space"])
            node_data["diskSpaceFree"] = float(historic_node_dataframe["node free disk space"])
    del historic_node_dataframe
    return node_data

async def add_pair_count(layer: int, node_data: dict, node_cluster_data: dict, configuration: dict) -> dict:
    node_data["nodePairCount"] = len(node_cluster_data)
    for node_pair in node_cluster_data:
        if f"layer {layer}" in configuration["source ids"].keys():
            for cluster_layer in configuration["source ids"].keys():
                if cluster_layer == f"layer {layer}":
                    for cluster_name, cluster_id in configuration["source ids"][cluster_layer].items():
                        if cluster_id == node_pair["id"]:
                            node_data["clusterNames"] = cluster_name.lower()


    return node_data