
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
