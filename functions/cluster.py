"""CHECK IF PREVIOUSLY REQUESTED"""


async def check(node_data, configuration):
    previously_requested_clusters = []
    previously_requested_clusters_data = []
    for k, v in node_data.items():
        if k == "clusterNames":
            for item in v:
                if item in list(configuration["source ids"].keys()):
                    if item not in previously_requested_clusters:
                        print(f"Request data for {item}")
                        previously_requested_clusters.append(item)
                        previously_requested_clusters = list(set(previously_requested_clusters))
        if k == "formerClusterNames":
            for item in v:
                if item in list(configuration["source ids"].keys()):
                    if item not in previously_requested_clusters:
                        print(f"Request data for {item}")
                        previously_requested_clusters.append(item)
                        previously_requested_clusters = list(set(previously_requested_clusters))