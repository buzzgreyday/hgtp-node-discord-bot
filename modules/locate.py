async def node(node_data, all_supported_clusters_data):
    for lst in all_supported_clusters_data:
        for cluster in lst:
            if f"layer {node_data['layer']}" == cluster["layer"]:
                for pair in cluster["pair data"]:
                    # LATER INCLUDE ID WHEN SUBSCRIBING
                    if pair["ip"] == node_data["host"]:
                        node_data["clusterNames"] = cluster["cluster name"].lower()

    return node_data

async def historic_node_data(dask_client, node_data: dict, history_dataframe):
    # ISOLATE LAYER AND NODE IN HISTORIC DATA
    ip = None
    port = None
    for k, v in node_data.items():
        if k == "host":
            ip = v
        if k == "publicPort":
            port = v
    # LATER USE NODE ID
    historic_node_dataframe = await dask_client.compute(history_dataframe[(history_dataframe["node ip"] == ip) & (history_dataframe["node port"] == port)])
    del ip, port, k, v
    return historic_node_dataframe


async def former_historic_node_data(historic_node_dataframe):
    return historic_node_dataframe[historic_node_dataframe["index timestamp"] == historic_node_dataframe["index timestamp"].max()]

async def registered_subscriber_node_data(dask_client, ip: str, subscriber_dataframe) -> dict:
    subscriber = {"name": await dask_client.compute(subscriber_dataframe.name[subscriber_dataframe.ip == ip]),
                  "contact": await dask_client.compute(subscriber_dataframe.contact[subscriber_dataframe.ip == ip]),
                  "ip": ip,
                  "public_l0": tuple(await dask_client.compute(subscriber_dataframe.public_l0[subscriber_dataframe.ip == ip])),
                  "public_l1": tuple(await dask_client.compute(subscriber_dataframe.public_l1[subscriber_dataframe.ip == ip]))}

    return subscriber