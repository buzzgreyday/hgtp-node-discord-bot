async def merge_node_data(node_data,  validator_mainnet_data, validator_testnet_data, all_supported_clusters_data):
    async def locate_in_validator_data(node_data) -> tuple[bool, dict]:
        async def set_variables(node_data):
            node_data["nodeWalletAddress"] = validator["address"]
            node_data["id"] = validator["id"]
            return node_data

        for network in [validator_mainnet_data, validator_testnet_data]:
            for validator in network:
                for node_ip, node_id in zip(validator["ip"].split(), validator["id"].split()):
                    if node_ip == str(node_data["host"]):
                        print("FOUND IP")
                        node_data = await set_variables(node_data)
                        located = True
                    elif node_id == str(node_data["id"]):
                        print("FOUND ID")
                        node_data = await set_variables(node_data)
                        located = True
        return located, node_data

    """DECIDE WHAT TO CHECK"""
    located = False
    # YOU NEED THIS VALUE DECLARED SO SIZE DOESN'T CHANGE DURING ITERATION
    # YOU MIGHT WANT TO DECLARE ALL VALUES FIRST TIME THE DICT IS CREATED

    for k, v in node_data.items():
        if k == "clusterNames":
            for all_latest_cluster_data in all_supported_clusters_data:
                if (f"layer {node_data['layer']}" == all_latest_cluster_data["layer"]) and (all_latest_cluster_data["cluster name"] == v):
                    node_data["clusterState"] = str(all_latest_cluster_data["state"])
                    node_data["clusterPairCount"] = len(all_latest_cluster_data["data"])
                    # MAKE SOME CONDITIONS TO ENHANCE SPEED
                    if not located:
                        located, node_data = await locate_in_validator_data(node_data)

        if k == "formerClusterNames":
            for all_former_cluster_data in all_supported_clusters_data:
                if (f"layer {node_data['layer']}" == all_former_cluster_data["layer"]) and (all_former_cluster_data["cluster name"] == v):
                    node_data["formerClusterState"] = str(all_former_cluster_data["state"])
                    node_data["formerClusterPairCount"] = len(all_former_cluster_data["data"])
                    if not located:
                        located, node_data = await locate_in_validator_data(node_data)

    return node_data
