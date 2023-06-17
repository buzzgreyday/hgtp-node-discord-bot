from assets.code import determine_module, history, user, cluster, dt, schemas
from assets.code.discord import discord


def merge_data(node_data: schemas.Node, cluster_data):
    if cluster_data is None:
        pass
    elif node_data.layer == cluster_data["layer"]:
        for peer in cluster_data["peer_data"]:
            if (peer["ip"] == node_data.ip) or (peer["id"] == node_data.id):
                node_data.cluster_name = cluster_data["name"].lower()
                node_data.latest_cluster_session = cluster_data["session"]
                node_data.cluster_version = cluster_data["version"]
                node_data.cluster_peer_count = cluster_data["peer_count"]
                node_data.cluster_state = cluster_data["state"]
                # Because we know the IP and ID, we can auto-recognize changing publicPort and later update
                # the subscription based on the node_data values
                if node_data.public_port != peer["publicPort"]:
                    node_data.public_port = peer["publicPort"]
                break

    return node_data


def data_template(requester, subscriber, port: int, layer: int, latest_tessellation_version: str, dt_start):
    return {
        "name": subscriber["name"][subscriber.public_port == port].values[0],
        "contact": subscriber["contact"][subscriber.public_port == port].values[0],
        "host": subscriber["ip"][subscriber.public_port == port].values[0],
        "layer": layer,
        "publicPort": port,
        "p2pPort": None,
        "id": subscriber["id"][subscriber.public_port == port].values[0],
        "nodeWalletAddress": None,
        "nodeWalletBalance": None,
        "clusterNames": None,
        "formerClusterNames": None,
        "state": None,
        "clusterState": None,
        "formerClusterState": None,
        "clusterConnectivity": None,
        "formerClusterConnectivity": None,
        "rewardState": None,
        "formerRewardState": None,
        "rewardTrueCount": None,
        "rewardFalseCount": None,
        "clusterAssociationTime": None,
        "formerClusterAssociationTime": None,
        "clusterDissociationTime": None,
        "formerClusterDissociationTime": None,
        "nodeClusterSession": None,
        "latestClusterSession": None,
        "nodePeerCount": None,
        "clusterPeerCount": None,
        "formerClusterPeerCount": None,
        "version": None,
        "clusterVersion": None,
        "latestVersion": latest_tessellation_version,
        "cpuCount": None,
        "diskSpaceTotal": None,
        "diskSpaceFree": None,
        "1mSystemLoadAverage": None,
        "notify": False if requester is None else True,
        "lastNotifiedTimestamp": None,
        "timestampIndex": dt_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "formerTimestampIndex": None

    }


async def check(dask_client, bot, process_msg, requester, subscriber, port, layer, latest_tessellation_version: str,
                history_dataframe, all_cluster_data: list[dict], dt_start, configuration: dict) -> tuple:
    process_msg = await discord.update_request_process_msg(process_msg, 2, None)
    node_data = schemas.Node(name=subscriber["name"][subscriber.public_port == port].values[0],
                             contact=subscriber["contact"][subscriber.public_port == port].values[0],
                             ip=subscriber["ip"][subscriber.public_port == port].values[0],
                             layer=layer,
                             public_port=port,
                             id=subscriber["id"][subscriber.public_port == port].values[0],
                             latest_version=latest_tessellation_version,
                             notify=False if requester is None else True,
                             timestamp_index=dt.datetime.utcnow())

    # node_data = data_template(requester, subscriber, port, layer, latest_tessellation_version, dt_start)
    loc_timer_start = dt.timing()[1]
    cluster_data = cluster.locate_node(node_data, all_cluster_data)
    loc_timer_stop = dt.timing()[1]
    print("LOCATE NODE:", loc_timer_stop - loc_timer_start)
    node_data = merge_data(node_data, cluster_data)
    historic_node_dataframe = await history.node_data(dask_client, node_data, history_dataframe)
    historic_node_dataframe = history.former_node_data(historic_node_dataframe)
    node_data = history.merge_data(node_data, cluster_data, historic_node_dataframe)
    process_msg = await discord.update_request_process_msg(process_msg, 3, None)
    # HERE YOU ALSO NEED A DEFAULT CLUSTER MODULE? THINK ABOUT WHAT SUCH A MODULE COULD CONTRIBUTE WITH
    node_data, process_msg = await cluster.get_module_data(process_msg, node_data, configuration)
    name = node_data.cluster_name if node_data.cluster_name is not None else node_data.former_cluster_name
    if name is not None and configuration["modules"][name][node_data.layer]["rewards"]:
        node_data = determine_module.set_module(node_data.cluster_name, configuration).check_rewards(node_data, cluster_data)
    await user.update_public_port(dask_client, node_data)

    return node_data, process_msg
