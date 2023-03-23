async def merge_node_data(node_data,  validator_mainnet_data, validator_testnet_data, all_supported_clusters_data):

    """DECIDE WHAT TO CHECK
    ADD VALIDATOR CHECK HERE?"""
    # YOU NEED THIS VALUE DECLARED SO SIZE DOESN'T CHANGE DURING ITERATION
    # YOU MIGHT WANT TO DECLARE ALL VALUES FIRST TIME THE DICT IS CREATED
    for lst in all_supported_clusters_data:
        for cluster in lst:
            if cluster["layer"] == f"layer {node_data['layer']}":
                if cluster["cluster name"] == node_data["clusterNames"]:
                    node_data["clusterPairCount"] = cluster["pair count"]
                    node_data["clusterState"] = cluster["state"]
                if cluster["cluster name"] == node_data["formerClusterNames"]:
                    node_data["formerClusterPairCount"] = cluster["pair count"]
                    node_data["formerClusterState"] = cluster["state"]

    for list_of_dict in [validator_mainnet_data, validator_testnet_data]:
        for validator in list_of_dict:
            if validator["ip"] == node_data["host"]:
                node_data["nodeWalletAddress"] = validator["address"]
                break
            elif validator["id"] == node_data["id"]:
                node_data["nodeWalletAddress"] = validator["address"]
                break
        break
    return node_data
