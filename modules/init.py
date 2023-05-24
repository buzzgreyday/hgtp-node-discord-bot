from modules import subscription, node, history, rewards, determine_module
from modules.discord import discord


async def check(dask_client, bot, process_msg, requester, subscriber, port, layer, latest_tessellation_version: str,
                history_dataframe, cluster_data: list[dict], dt_start, configuration: dict) -> tuple:
    process_msg = await discord.update_request_process_msg(process_msg, 2, None)
    node_data = node.data_template(requester, subscriber, port, layer, latest_tessellation_version, dt_start)
    cluster_data_idx, cluster_idx = node.locate_cluster_data_position(node_data, cluster_data)
    node_data = node.merge_cluster_data(node_data, cluster_data, cluster_data_idx, cluster_idx)
    historic_node_dataframe = await history.node_data(dask_client, node_data, history_dataframe)
    historic_node_dataframe = history.former_node_data(historic_node_dataframe)
    node_data = history.merge(node_data, historic_node_dataframe)
    node_data = node.merge_general_cluster_data(node_data, cluster_data)
    process_msg = await discord.update_request_process_msg(process_msg, 3, None)
    node_data, process_msg = await node.get_cluster_module_data(process_msg, node_data, configuration)
    if determine_module.set_module(node_data['clusterNames'], configuration) in ("mainnet", "testnet"):
        node_data = rewards.check(node_data, configuration)
    await subscription.update_public_port(dask_client, node_data)

    return node_data, process_msg
