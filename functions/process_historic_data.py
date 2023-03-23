async def isolate_node_data(dask_client, node_data: dict, history_dataframe):
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


async def isolate_former_node_data(historic_node_dataframe):
    return historic_node_dataframe[historic_node_dataframe["index timestamp"] == historic_node_dataframe["index timestamp"].max()]



