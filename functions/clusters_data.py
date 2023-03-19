async def merge_node_data(node_data, all_supported_clusters_data):
    """DECIDE WHAT TO CHECK"""
    cluster_state = []
    former_cluster_state = []
    for k, v in node_data.items():
        if k == "clusterNames":
            for cluster_name in v:
                for all_latest_cluster_data in all_supported_clusters_data:
                    if (f"layer {node_data['layer']}" == all_latest_cluster_data["layer"]) and (all_latest_cluster_data["cluster name"] == cluster_name):
                        cluster_state.append(str(all_latest_cluster_data["state"]))
                        for cluster_data in all_latest_cluster_data["data"]:
                            pass
                            """if node_data["id"] == cluster_data["id"]:
                                print(cluster_data["id"])"""
        if k == "formerClusterNames":
            for former_cluster_name in v:
                for all_former_cluster_data in all_supported_clusters_data:
                    if (f"layer {node_data['layer']}" == all_former_cluster_data["layer"]) and (all_former_cluster_data["cluster name"] == former_cluster_name):
                        former_cluster_state.append(str(all_former_cluster_data["state"]))
                        for cluster_data in all_former_cluster_data["data"]:
                            pass
                            """if node_data["id"] == cluster_data["id"]:
                                print(cluster_data["id"])"""
    node_data["clusterState"] = cluster_state
    node_data["formerClusterState"] = former_cluster_state
    return node_data