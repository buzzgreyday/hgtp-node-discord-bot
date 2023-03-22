async def merge_node_data(node_data,  validator_mainnet_data, validator_testnet_data, all_supported_clusters_data):

    """DECIDE WHAT TO CHECK
    ADD VALIDATOR CHECK HERE?"""
    # YOU NEED THIS VALUE DECLARED SO SIZE DOESN'T CHANGE DURING ITERATION
    # YOU MIGHT WANT TO DECLARE ALL VALUES FIRST TIME THE DICT IS CREATED
    for cluster in all_supported_clusters_data:
        for data in cluster:
            if data["layer"] == f"layer {node_data['layer']}":
                if data["cluster name"] == node_data["clusterNames"]:
                    node_data["clusterPairCount"] = data["pair count"]
                    node_data["clusterState"] = data["state"]
                if data["cluster name"] == node_data["formerClusterNames"]:
                    node_data["formerClusterPairCount"] = data["pair count"]
                    node_data["formerClusterState"] = data["state"]

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
