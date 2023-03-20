async def merge_node_data(node_data,  validator_mainnet_data, validator_testnet_data, all_supported_clusters_data):

    """DECIDE WHAT TO CHECK
    ADD VALIDATOR CHECK HERE?"""
    located = False
    # YOU NEED THIS VALUE DECLARED SO SIZE DOESN'T CHANGE DURING ITERATION
    # YOU MIGHT WANT TO DECLARE ALL VALUES FIRST TIME THE DICT IS CREATED

    for k, v in node_data.items():
        if k == "clusterNames":
            for all_latest_cluster_data in all_supported_clusters_data:
                if (f"layer {node_data['layer']}" == all_latest_cluster_data["layer"]) and (all_latest_cluster_data["cluster name"] == v):
                    node_data["clusterState"] = str(all_latest_cluster_data["state"])
                    node_data["clusterPairCount"] = len(all_latest_cluster_data["data"])

        if k == "formerClusterNames":
            for all_former_cluster_data in all_supported_clusters_data:
                if (f"layer {node_data['layer']}" == all_former_cluster_data["layer"]) and (all_former_cluster_data["cluster name"] == v):
                    node_data["formerClusterState"] = str(all_former_cluster_data["state"])
                    node_data["formerClusterPairCount"] = len(all_former_cluster_data["data"])

    return node_data
