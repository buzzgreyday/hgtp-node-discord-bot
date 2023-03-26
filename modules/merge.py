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

def historic_data(node_data: dict, historic_node_dataframe) -> dict:

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

def cluster_agnostic_node_data(node_data,  validator_mainnet_data, validator_testnet_data, all_supported_clusters_data):

    for lst in all_supported_clusters_data:
        for cluster in lst:
            if cluster["layer"] == f"layer {node_data['layer']}":
                if cluster["cluster name"] == node_data["clusterNames"]:
                    node_data["clusterPeerCount"] = cluster["peer count"]
                    node_data["clusterState"] = cluster["state"]
                if cluster["cluster name"] == node_data["formerClusterNames"]:
                    node_data["formerClusterPeerCount"] = cluster["peer count"]
                    node_data["formerClusterState"] = cluster["state"]

    for list_of_dict in [validator_mainnet_data, validator_testnet_data]:
        for validator in list_of_dict:
            if validator["ip"] == node_data["host"]:
                node_data["nodeWalletAddress"] = validator["address"]
                break
            elif validator["id"] == node_data["id"]:
                node_data["nodeWalletAddress"] = validator["address"]
                break
    return node_data