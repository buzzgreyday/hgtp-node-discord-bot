async def merge_node_data(node_data,  validator_mainnet_data, validator_testnet_data, all_supported_clusters_data):
    async def locate_in_validator_data(node_data) -> bool:
        async def set_variables(node_data):
            node_data["nodeWalletAddress"] = validator["address"]
            node_data["id"] = validator["id"]
            return node_data

        located = False
        for network in [validator_mainnet_data, validator_testnet_data]:
            for validator in network:
                for node_ip, node_id in zip(validator["ip"].split(), validator["id"].split()):
                    if node_ip == str(node_data["host"]):
                        node_data = await set_variables(node_data)
                        print(f"{node_ip} FOUND IN VALIDATOR DATA")
                        located = True
                    elif node_id == str(node_data["id"]):
                        node_data = await set_variables(node_data)
                        print(f"{node_id} FOUND IN VALIDATOR DATA")
                        located = True
        return located

    """DECIDE WHAT TO CHECK"""
    cluster_state = []
    cluster_pair_count = []
    former_cluster_state = []
    former_cluster_pair_count = []
    node_data["nodeWalletAddress"] = None
    located = False
    for k, v in node_data.items():
        if k == "clusterNames":
            for cluster_name in v:
                for all_latest_cluster_data in all_supported_clusters_data:
                    if (f"layer {node_data['layer']}" == all_latest_cluster_data["layer"]) and (all_latest_cluster_data["cluster name"] == cluster_name):
                        cluster_state.append(str(all_latest_cluster_data["state"]))
                        cluster_pair_count.append(len(all_latest_cluster_data["data"]))
                        # MAKE SOME CONDITIONS TO ENHANCE SPEED
                        if not located:
                            located = await locate_in_validator_data(node_data)
                        for cluster_data in all_latest_cluster_data["data"]:
                            pass
                            """if node_data["id"] == cluster_data["id"]:
                                print(cluster_data["id"])"""
        if k == "formerClusterNames":
            for former_cluster_name in v:
                for all_former_cluster_data in all_supported_clusters_data:
                    if (f"layer {node_data['layer']}" == all_former_cluster_data["layer"]) and (all_former_cluster_data["cluster name"] == former_cluster_name):
                        former_cluster_state.append(str(all_former_cluster_data["state"]))
                        former_cluster_pair_count.append(len(all_former_cluster_data["data"]))
                        if not located:
                            located = await locate_in_validator_data(node_data)
                        for cluster_data in all_former_cluster_data["data"]:
                            pass
                            """if node_data["id"] == cluster_data["id"]:
                                print(cluster_data["id"])"""
    node_data["clusterState"] = cluster_state
    node_data["formerClusterState"] = former_cluster_state
    node_data["clusterPairCount"] = cluster_pair_count
    node_data["formerClusterPairCount"] = former_cluster_pair_count

    return node_data
