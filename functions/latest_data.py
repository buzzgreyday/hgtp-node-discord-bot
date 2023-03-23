async def locate_node(node_data, all_supported_clusters_data):
    for lst in all_supported_clusters_data:
        for cluster in lst:
            if f"layer {node_data['layer']}" == cluster["layer"]:
                for pair in cluster["pair data"]:
                    # LATER INCLUDE ID WHEN SUBSCRIBING
                    if pair["ip"] == node_data["host"]:
                        node_data["clusterNames"] = cluster["cluster name"].lower()
                        print(f"NODE WAS FOUND IN {cluster['cluster name'].upper()} {cluster['layer'].upper()}")

    return node_data
