async def merge_node_data(node_data,  validator_mainnet_data, validator_testnet_data, all_supported_clusters_data):

    """DECIDE WHAT TO CHECK
    ADD VALIDATOR CHECK HERE?"""
    # YOU NEED THIS VALUE DECLARED SO SIZE DOESN'T CHANGE DURING ITERATION
    # YOU MIGHT WANT TO DECLARE ALL VALUES FIRST TIME THE DICT IS CREATED

    for k, v in node_data.items():
        if k == "clusterNames":
            cluster_name = str(v)
        if k == "formerClusterNames":
            former_cluster_name = str(v)

    for cluster_data in all_supported_clusters_data:
        print(cluster_data.keys())
        for k, v in cluster_data.items():
            if (k == "layer") and (v == f"layer {node_data['layer']}"):
                print("LAYER OK")
            if (k == "cluster name") and (v == cluster_name):
                print("KEY OK")
                node_data["clusterState"] = str(cluster_data["state"])
                print(node_data["clusterState"])
                node_data["clusterPairCount"] = len(cluster_data["data"])
            if (k == "cluster name") and (v == former_cluster_name):
                node_data["formerClusterState"] = str(cluster_data["state"])
                node_data["formerClusterPairCount"] = len(cluster_data["data"])

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
