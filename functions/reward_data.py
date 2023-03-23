async def check(node_data: dict, all_supported_clusters_data: list):
    # SAME PROCEDURE AS IN CLUSTERS_DATA.MERGE_NODE_DATA
    for cluster in all_supported_clusters_data:
        for data in cluster:
            if (data["layer"] == f"layer {node_data['layer']}") and (data["cluster name"] == node_data["clusterNames"]):
                if str(node_data["nodeWalletAddress"]) in data["recently rewarded"]:
                    node_data["rewardState"] = True
                elif (data["recently rewarded"] is None) and (str(node_data["nodeWalletAddress"]) not in data["recently rewarded"]):
                        node_data["rewardState"] = False

    return node_data