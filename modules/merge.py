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
    # Might need refactoring when metagraphs is coming
    if not historic_node_dataframe.empty:
        print(historic_node_dataframe["clusterNames"])
        node_data["formerClusterNames"] = Clean(historic_node_dataframe["clusterNames"]).make_lower()
        node_data["formerClusterConnectivity"] = Clean(historic_node_dataframe["clusterConnectivity"][historic_node_dataframe["clusterNames"] == node_data["formerClusterNames"]]).make_none()
        node_data["formerClusterAssociationTime"] = Clean(historic_node_dataframe["clusterAssociationTime"][historic_node_dataframe["clusterNames"] == node_data["formerClusterNames"]]).make_none()
        node_data["formerClusterDissociationTime"] = Clean(historic_node_dataframe["clusterDissociationTime"][historic_node_dataframe["clusterNames"] == node_data["formerClusterNames"]]).make_none()
        node_data["formerTimestampIndex"] = Clean(historic_node_dataframe["timestampIndex"]).make_none()
        node_data["lastNotifiedTimestamp"] = Clean(historic_node_dataframe["lastNotifiedTimestamp"]).make_none()
        if node_data["state"] == "Offline":
            node_data["id"] = Clean(historic_node_dataframe["id"]).make_none()
            node_data["nodeWalletAddress"] = Clean(historic_node_dataframe["nodeWalletAddress"]).make_none()
            node_data["version"] = Clean(historic_node_dataframe["version"]).make_none()
            node_data["cpuCount"] = float(historic_node_dataframe["cpuCount"])
            node_data["diskSpaceTotal"] = float(historic_node_dataframe["diskSpaceTotal"])
            node_data["diskSpaceFree"] = float(historic_node_dataframe["diskSpaceFree"])
    return node_data


def cluster_agnostic_node_data(node_data, all_supported_clusters_data):

    for lst in all_supported_clusters_data:
        for cluster in lst:
            if cluster["layer"] == f"layer {node_data['layer']}":
                if cluster["cluster name"] == node_data["clusterNames"]:
                    node_data["clusterPeerCount"] = cluster["peer count"]
                    node_data["clusterState"] = cluster["state"]
                if cluster["cluster name"] == node_data["formerClusterNames"]:
                    node_data["formerClusterPeerCount"] = cluster["peer count"]
                    node_data["formerClusterState"] = cluster["state"]

    return node_data