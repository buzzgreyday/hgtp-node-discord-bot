async def merge(layer, latest_tessellation_version, node_data, cluster_data, configuration):
    lb_ids = []
    node_data["latestVersion"] = latest_tessellation_version
    node_data["layer"] = layer
    node_data["clusterPairs"] = [len(cluster_data)]
    node_data["clusterNames"] = []
    if cluster_data:
        lb_ids.extend(v for k, v in configuration["source ids"].items())
        for d in cluster_data:
            for k, v in d.items():
                if (k == "id") and (str(v) in lb_ids):
                    for node_cluster_name, node_id in configuration["source ids"].items():
                        if node_id == str(v):
                            node_data["clusterNames"].append(node_cluster_name)
                    del node_cluster_name, lb_ids
        cluster_data.clear()
        del lb_ids, cluster_data, k, v, d
    return node_data


async def check(node_data, all_supported_clusters_data):
    """DECIDE WHAT TO CHECK"""
    clusters = []
    for k, v in node_data.items():
        if k == "clusterNames":
            clusters.extend(v)
        if k == "formerClusterNames":
            clusters.extend(v)
        clusters = list(set(clusters))
    for dictionary in all_supported_clusters_data:

        for item in dictionary["data"]:
            print(item["id"])
        """if f"layer {node_data['layer']}" in cluster_layer:
            print("name in cluster_layer_dictionary")
            for cluster_layer_dictionary in cluster_layer_list_of_dictionaries:
                print(cluster_layer_dictionary)
                for cluster_name, cluster_name_dictionary in cluster_layer_dictionary.items():
                    print(cluster_name, cluster_name_dictionary.keys())"""


