from assets.src import schemas, history, dt, cluster, determine_module
from assets.src.discord import discord


def merge_data(node_data: schemas.Node, found: bool, cluster_data: dict):
    if not found and cluster_data is not None:
        print(cluster_data)
        node_data.last_known_cluster_name = cluster_data["name"]
        node_data.latest_cluster_session = cluster_data["session"]
        node_data.cluster_version = cluster_data["version"]
        node_data.cluster_peer_count = cluster_data["peer_count"]
        node_data.cluster_state = cluster_data["state"]
    elif found and cluster_data is not None:
        if node_data.layer == cluster_data["layer"]:
            node_data.cluster_name = cluster_data["name"]
            node_data.latest_cluster_session = cluster_data["session"]
            node_data.cluster_version = cluster_data["version"]
            node_data.cluster_peer_count = cluster_data["peer_count"]
            node_data.cluster_state = cluster_data["state"]

    return node_data


async def check(bot, process_msg, requester, subscriber, port, layer, latest_tessellation_version: str,
                all_cluster_data: list[dict], dt_start, configuration: dict) -> tuple:
    process_msg = await discord.update_request_process_msg(process_msg, 2, None)
    node_data = schemas.Node(name=subscriber.name.values[0],
                             contact=subscriber.contact.values[0],
                             ip=subscriber.ip.values[0],
                             layer=subscriber.layer.values[0],
                             public_port=subscriber.public_port.values[0],
                             id=subscriber.id.values[0],
                             wallet_address=subscriber.wallet.values[0],
                             latest_version=latest_tessellation_version,
                             notify=False if requester is None else True,
                             timestamp_index=dt.datetime.utcnow())
    # node_data = data_template(requester, subscriber, port, layer, latest_tessellation_version, dt_start)
    loc_timer_start = dt.timing()[1]
    node_data = await history.node_data(node_data, configuration)
    found_in_cluster, cluster_data = cluster.locate_node(node_data, all_cluster_data)
    loc_timer_stop = dt.timing()[1]
    """if cluster_data is None:
        print(node_data.former_cluster_name, node_data.ip, node_data.layer)
        exit(1)"""
    node_data = merge_data(node_data, found_in_cluster, cluster_data)
    process_msg = await discord.update_request_process_msg(process_msg, 3, None)
    # HERE YOU ALSO NEED A DEFAULT CLUSTER MODULE? THINK ABOUT WHAT SUCH A MODULE COULD CONTRIBUTE WITH
    node_data, process_msg = await cluster.get_module_data(process_msg, node_data, configuration)
    if node_data.cluster_name is not None and cluster_data is not None and configuration["modules"][node_data.cluster_name][node_data.layer]["rewards"]:
        node_data = determine_module.set_module(node_data.cluster_name, configuration).check_rewards(node_data, cluster_data)
    elif node_data.former_cluster_name is not None and cluster_data is not None and configuration["modules"][node_data.former_cluster_name][node_data.layer]["rewards"]:
        node_data = determine_module.set_module(node_data.former_cluster_name, configuration).check_rewards(node_data, cluster_data)
    elif node_data.last_known_cluster_name is not None and cluster_data is not None and configuration["modules"][node_data.last_known_cluster_name][node_data.layer]["rewards"]:
        node_data = determine_module.set_module(node_data.last_known_cluster_name, configuration).check_rewards(node_data, cluster_data)

    return node_data, process_msg
