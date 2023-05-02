def node_cluster(node_data, all_supported_clusters_data):
    for lst in all_supported_clusters_data:
        for cluster in lst:
            if int(node_data['layer']) == int(cluster["layer"][-1:]):
                for peer in cluster["peer data"]:
                    # LATER INCLUDE ID WHEN SUBSCRIBING
                    if peer["ip"] == node_data["host"]:
                        node_data["clusterNames"] = cluster["cluster name"].lower()
                        node_data["latestClusterSession"] = cluster["cluster session"]
                        node_data["clusterVersion"] = cluster["version"]
                        node_data["publicPort"] = peer["publicPort"]

    return node_data

async def historic_node_data(dask_client, node_data: dict, history_dataframe):
    return await dask_client.compute(history_dataframe[(history_dataframe["host"] == node_data["host"]) & (history_dataframe["publicPort"] == node_data["publicPort"])])

def former_historic_node_data(historic_node_dataframe):
    return historic_node_dataframe[historic_node_dataframe["timestampIndex"] == historic_node_dataframe["timestampIndex"].max()]

async def registered_subscriber_node_data(dask_client, ip: str, subscriber_dataframe) -> dict:
    subscriber = {
        "name": await dask_client.compute(subscriber_dataframe.name[subscriber_dataframe.ip == ip]),
        "contact": await dask_client.compute(subscriber_dataframe.contact[subscriber_dataframe.ip == ip]),
        "ip": ip,
        "public_l0": tuple(await dask_client.compute(subscriber_dataframe.public_l0[subscriber_dataframe.ip == ip])),
        "public_l1": tuple(await dask_client.compute(subscriber_dataframe.public_l1[subscriber_dataframe.ip == ip]))}

    return subscriber