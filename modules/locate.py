def node_cluster(node_data, all_supported_clusters_data):
    for lst in all_supported_clusters_data:
        for cluster in lst:
            if int(node_data['layer']) == int(cluster["layer"].split(' ')[-1]):
                for peer in cluster["peer data"]:
                    # LATER INCLUDE ID WHEN SUBSCRIBING
                    if (peer["ip"] == node_data["host"]) and (peer["id"] == node_data["id"]):
                        node_data["clusterNames"] = cluster["cluster name"].lower()
                        node_data["latestClusterSession"] = cluster["cluster session"]
                        node_data["clusterVersion"] = cluster["version"]
                        # Because we know the IP and ID, we can auto-recognize changing publicPort and later update
                        # the subscription based on the node_data values
                        if node_data["publicPort"] != peer["publicPort"]:
                            node_data["publicPort"] = peer["publicPort"]
    return node_data


async def historic_node_data(dask_client, node_data: dict, history_dataframe):

    # history_node_dataframe = await dask_client.compute(history_dataframe[(history_dataframe["host"] == node_data["host"]) & (history_dataframe["publicPort"] == node_data["publicPort"])])
    if node_data["publicPort"] is not None:
        return await dask_client.compute(history_dataframe[(history_dataframe["host"] == node_data["host"]) & (history_dataframe["publicPort"] == float(node_data["publicPort"]))])
    else:
        return await dask_client.compute(history_dataframe[(history_dataframe["host"] == node_data["host"]) & (history_dataframe["publicPort"] == node_data["publicPort"])])


def former_historic_node_data(historic_node_dataframe):
    return historic_node_dataframe[historic_node_dataframe["timestampIndex"] == historic_node_dataframe["timestampIndex"].max()]


async def registered_subscriber_node_data(dask_client, bot, node_id: str, subscriber_dataframe):
    """contact = await dask_client.compute(subscriber_dataframe.contact[subscriber_dataframe.ip == ip])
        return {
            "name": bot.get_user(contact.values[0]),
            "contact": contact.values[0],
            "ip": ip,
            "ids": tuple(set(await dask_client.compute(subscriber_dataframe.id[(subscriber_dataframe.ip == ip) & (subscriber_dataframe.layer == 0)]) + await dask_client.compute(subscriber_dataframe.id[(subscriber_dataframe.ip == ip) & (subscriber_dataframe.layer == 1)]))),
            "public_zero_ports": tuple(await dask_client.compute(subscriber_dataframe.public_port[(subscriber_dataframe.ip == ip) & (subscriber_dataframe.layer == 0)])),
            "public_one_ports": tuple(await dask_client.compute(subscriber_dataframe.public_port[(subscriber_dataframe.ip == ip) & (subscriber_dataframe.layer == 1)]))}
        """
    # THIS SHOULD BE DETERMINED BY ID
    return await dask_client.compute(subscriber_dataframe[subscriber_dataframe.id == node_id])
