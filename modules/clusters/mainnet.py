#######################################################################################################################
#                       |    -**  MAINNET HGTP NODE SPIDER BOT MODULE, VERSION 1.0  **-    |
# --------------------------------------------------------------------------------------------------------------------
#  + DESCRIPTION
#   THIS MODULE CONTAINS PROJECT- OR BUSINESS-SPECIFIC CODE WHICH ENABLES SUPPORT FOR THIS PARTICULAR CLUSTER'S API.
# --------------------------------------------------------------------------------------------------------------------
#######################################################################################################################
# * IMPORTS: MODULES, CONSTANTS AND VARIABLES
# ---------------------------------------------------------------------------------------------------------------------

import asyncio
from datetime import datetime

import nextcord

from modules.clusters import all
from modules import request, encode

"""
    SECTION 1: PRELIMINARIES
"""
# ---------------------------------------------------------------------------------------------------------------------
# + CLUSTER SPECIFIC FUNCTIONS AND CLASSES GOES HERE
# ---------------------------------------------------------------------------------------------------------------------
#   THE FUNCTION BELOW IS ONE OF THE FIRST INITIATIONS. THIS FUNCTION REQUESTS DATA FROM THE MAINNET/TESTNET CLUSTER.
#   IN THIS MODULE WE REQUEST THINGS LIKE STATE, LOAD BALANCER ID, PEERS AND THE LATEST CLUSTER SESSION TOKEN.
#   WE THEN AGGREAGATE ALL THIS DATA IN A "CLUSTER DICTIONARY" AND ADDS IT TO A LIST OF ALL THE SUPPORTED CLUSTERS.
#   WE ALSO CHECK FOR REWARDS.
# ---------------------------------------------------------------------------------------------------------------------

async def request_cluster_data(lb_url, cluster_layer, cluster_name, configuration):
    cluster_resp = await request.safe(
        f"{lb_url}/{configuration['request']['url']['clusters']['url endings']['cluster info']}", configuration)
    node_resp = await request.safe(
        f"{lb_url}/{configuration['request']['url']['clusters']['url endings']['node info']}", configuration)
    latest_ordinal, latest_timestamp, addresses = await locate_rewarded_addresses(cluster_layer, cluster_name,
                                                                                          configuration)

    if node_resp is None:
        cluster_state = "offline" ; cluster_id = await all.locate_id_offline(cluster_layer, cluster_name, configuration) ; cluster_session = None
    else:
        cluster_state = str(node_resp['state']).lower() ; cluster_id = node_resp["id"] ; cluster_session = node_resp["clusterSession"]

    cluster = {
        "layer": cluster_layer,
        "cluster name": cluster_name,
        "state": cluster_state,
        "id": cluster_id,
        "peer count": len(cluster_resp),
        "cluster session": cluster_session,
        "latest ordinal": latest_ordinal,
        "latest ordinal timestamp": latest_timestamp,
        "recently rewarded": addresses,
        "peer data": cluster_resp
    }
    await all.update_config_with_latest_values(cluster, configuration)
    del node_resp
    return cluster

# THE ABOVE FUNCTION ALSO REQUEST THE MOST RECENT REWARDED ADDRESSES. THIS FUNCTION LOCATES THESE ADDRESSES BY
# REQUESTING THE RELEVANT API'S.

# (!) YOU COULD MAKE 50 (MAGIC NUMBER) VARIABLE IN THE CONFIG YAML.
#     YOU MIGHT ALSO BE ABLE TO IMPROVE ON THE TRY/EXCEPT BLOCK LENGTH.

async def locate_rewarded_addresses(cluster_layer, cluster_name, configuration):
    try:
        latest_ordinal, latest_timestamp = \
            await request_snapshot(
                f"{configuration['request']['url']['block explorer'][cluster_layer][cluster_name]}"
                f"/global-snapshots/latest", configuration)
        tasks = []
        for ordinal in range(latest_ordinal-50, latest_ordinal):
            tasks.append(asyncio.create_task(request_reward_addresses_per_snapshot(
                f"{configuration['request']['url']['block explorer'][cluster_layer][cluster_name]}"
                f"/global-snapshots/{ordinal}/rewards", configuration
            )))
        addresses = []
        for task in tasks:
            addresses.extend(await task); addresses = list(set(addresses))
    except KeyError:
        latest_ordinal = None; latest_timestamp = None; addresses = []
    return latest_ordinal, latest_timestamp, addresses

# IN THE FUNCTIOM ABOVE WE NEED TO REQUEST SNAPSHOT DATA, BEFORE BEING ABLE TO KNOW WHICH REWARD SNAPSHOTS WE WANT TO
# CHECK AGAINST. THIS IS DONE IN THE FUNCTION BELOW.

async def request_snapshot(request_url, configuration):
    data = await request.safe(request_url, configuration)
    if data is not None:
        ordinal = data["data"]["ordinal"]
        timestamp = datetime.strptime(data["data"]["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
        return ordinal, timestamp
    elif data is None:
        ordinal = None
        timestamp = None
        return ordinal, timestamp

async def request_reward_addresses_per_snapshot(request_url, configuration):
    data = await request.safe(request_url, configuration)
    return list(data_dictionary["destination"] for data_dictionary in data["data"])

"""
    SECTION 2: INDIVIDUAL NODE DATA PROCESSING
"""
# ---------------------------------------------------------------------------------------------------------------------
# + NODE SPECIFIC FUNCTIONS AND CLASSES GOES HERE
# ---------------------------------------------------------------------------------------------------------------------

async def node_cluster_data(node_data: dict, configuration: dict) -> tuple[dict, dict]:
    if node_data['publicPort'] is not None:
        node_info_data = await request.safe(
            f"http://{node_data['host']}:{node_data['publicPort']}/"
            f"{configuration['request']['url']['clusters']['url endings']['node info']}", configuration)
        node_data["state"] = "offline" if node_info_data is None else node_info_data["state"].lower()
        if node_info_data is not None:
            node_data["nodeClusterSession"] = node_info_data["clusterSession"]
            node_data["version"] = node_info_data["version"]
        if node_data["state"] != "offline":
            cluster_data = await request.safe(
                f"http://{str(node_data['host'])}:{str(node_data['publicPort'])}/"
                f"{str(configuration['request']['url']['clusters']['url endings']['cluster info'])}", configuration)
            metrics_data = await request.safe(
                f"http://{str(node_data['host'])}:{str(node_data['publicPort'])}/"
                f"{str(configuration['request']['url']['clusters']['url endings']['metrics info'])}", configuration)
            node_data["id"] = node_info_data["id"]
            node_data["nodeWalletAddress"] = encode.id_to_dag_address(node_data["id"])
            node_data["nodePeerCount"] = len(cluster_data) if cluster_data is not None else 0
            node_data.update(metrics_data)

        node_data = await request_wallet_data(node_data, configuration)
        node_data = set_connectivity_specific_node_data_values(node_data)
        node_data = set_association_time(node_data)

    return node_data


def reward_check(node_data: dict, all_supported_clusters_data: list):
    for lst in all_supported_clusters_data:
        for cluster in lst:
            # if (cluster["layer"] == f"layer {node_data['layer']}") and (cluster["cluster name"] == node_data["clusterNames"]):
            # if (cluster["cluster name"] == node_data["clusterNames"]) or (cluster["cluster name"] == node_data["formerClusterNames"]):
            if str(node_data["nodeWalletAddress"]) in cluster["recently rewarded"]:
                node_data["rewardState"] = True
            elif (cluster["recently rewarded"] is None) and (str(node_data["nodeWalletAddress"]) not in cluster["recently rewarded"]):
                    node_data["rewardState"] = False

    return node_data

async def request_wallet_data(node_data, configuration):
    for be_layer, be_names in configuration["request"]["url"]["block explorer"].items():
        if (node_data['clusterNames'] or node_data['formerClusterNames']) in list(be_names.keys()):
            for be_name, be_url in be_names.items():
                if be_name.lower() == (node_data['clusterNames'] or node_data['formerClusterNames']):
                    wallet_data = await request.safe(f"{be_url}/addresses/{node_data['nodeWalletAddress']}/balance", configuration)
                    if wallet_data is not None:
                        node_data["nodeWalletBalance"] = wallet_data["data"]["balance"]

        else:
            wallet_data = await request.safe(f"{configuration['request']['url']['block explorer']['layer 0']['mainnet']}/addresses/{node_data['nodeWalletAddress']}/balance", configuration)
            if wallet_data is not None:
                node_data["nodeWalletBalance"] = wallet_data["data"]["balance"]

    return node_data

"""
    SECTION 3: PROCESS AND CALCULATE CLUSTER SPECIFIC NODE DATA.
"""
# ---------------------------------------------------------------------------------------------------------------------
# + LIKE ASSOCIATION AND DISSOCIATION... FUNCTIONS WHICH SHOULD ONLY RUN IF A CLUSTER/MODULE EXISTS.
# ---------------------------------------------------------------------------------------------------------------------

def set_connectivity_specific_node_data_values(node_data):

    if node_data["formerClusterConnectivity"] is not None:
        if node_data["clusterNames"] != node_data["formerClusterNames"] \
            and node_data["formerClusterConnectivity"] in ["association", "new association"]:
            node_data["clusterConnectivity"] = "new dissociation"
        elif node_data["clusterNames"] == node_data["formerClusterNames"] \
            and node_data["formerClusterConnectivity"] in ["dissociation", "new dissociation"]:
            node_data["clusterConnectivity"] = "dissociated"
        elif node_data["clusterNames"] != node_data["formerClusterNames"] \
            and node_data["formerClusterConnectivity"] in ["disociation", "new dissociation"]:
            node_data["clusterConnectivity"] = "new associated"
        elif node_data["clusterNames"] == node_data["formerClusterNames"] \
            and node_data["formerClusterConnectivity"] in ["association", "new association"]:
            node_data["clusterConnectivity"] = "associated"
    elif node_data["formerClusterConnectivity"] is None:
        if node_data["clusterNames"] is None and node_data["formerClusterNames"] is not None \
                and node_data["nodeClusterSession"] != node_data["latestClusterSession"]:
            node_data["clusterConnectivity"] = "new dissociation"
        elif node_data["clusterNames"] is None and node_data["formerClusterNames"] is None:
            node_data["clusterConnectivity"] = "dissociated"

        elif node_data["clusterNames"] is not None and node_data["formerClusterNames"] is None \
                and node_data["nodeClusterSession"] == node_data["latestClusterSession"]:
            node_data["clusterConnectivity"] = "new association"
        elif node_data["clusterNames"] is not None and node_data["formerClusterNames"] is None \
                and node_data["nodeClusterSession"] != node_data["latestClusterSession"]:
            node_data["clusterConnectivity"] = "dissociated"

        elif node_data["clusterNames"] is not None and node_data["formerClusterNames"] is not None \
                and node_data["nodeClusterSession"] == node_data["latestClusterSession"]:
            node_data["clusterConnectivity"] = "associated"
        elif node_data["clusterNames"] is not None and node_data["formerClusterNames"] is not None \
                and node_data["nodeClusterSession"] != node_data["latestClusterSession"]:
            node_data["clusterConnectivity"] = "dissociated"

    return node_data

def set_association_time(node_data):
    if node_data["formerTimestampIndex"] is not None:
        # LINE BELOW IS TEMPORARY
        node_data["formerTimestampIndex"] = datetime.fromtimestamp(node_data["formerTimestampIndex"]).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        time_difference = (datetime.strptime(node_data["timestampIndex"], "%Y-%m-%dT%H:%M:%S.%fZ") - datetime.strptime(node_data["formerTimestampIndex"], "%Y-%m-%dT%H:%M:%S.%fZ")).seconds
    else:
        time_difference = datetime.strptime(node_data["timestampIndex"], "%Y-%m-%dT%H:%M:%S.%fZ").second

    node_data["clusterAssociationTime"] = node_data["formerClusterAssociationTime"] if node_data["formerClusterAssociationTime"] is not None else 0
    node_data["clusterDissociationTime"] = node_data["formerClusterDissociationTime"] if node_data["formerClusterDissociationTime"] is not None else 0

    if node_data["clusterConnectivity"] == "association":
        node_data["clusterAssociationTime"] = time_difference + node_data["formerClusterAssociationTime"]
        node_data["clusterDissociationTime"] = node_data["formerClusterDissociationTime"]
    elif node_data["clusterConnectivity"] == "disociation":
        node_data["clusterDissociationTime"] = time_difference + node_data["formerClusterDissociationTime"]
        node_data["clusterAssociationTime"] = node_data["formerClusterAssociationTime"]
    elif node_data["clusterConnectivity"] in ["new association", "new dissociation"]:
        node_data["clusterAssociationTime"] = node_data["formerClusterAssociationTime"]
        node_data["clusterDissociationTime"] = node_data["formerClusterDissociationTime"]
    return node_data

"""
    SECTION 4: CREATE REPORT
"""

def build_title(node_data):
    if node_data["clusterConnectivity"] == "new association":
        title_state = f"recently associated with \"{node_data['clusterNames'].title()}\""
    elif node_data["clusterConnectivity"] == "associated":
        title_state = f"associated with \"{node_data['clusterNames'].title()}\""
    elif node_data["clusterConnectivity"] == "new dissociation":
        title_state = f"recently dissociated from \"{node_data['formerClusterNames'].title()}\""
    elif node_data["clusterConnectivity"] == "dissociated":
        title_state = f"dissociated from \"{node_data['formerClusterNames'].title()}\""
    else:
        title_state = f"report"
    return f"{node_data['host']} layer {node_data['layer']} ({node_data['publicPort']}) {title_state}"


def build_general_node_state(node_data):
    def node_state_field():
        if node_data["id"] is not None:
            return f"{field_symbol} **NODE**\n" \
                   f"```\n" \
                   f"ID: {node_data['id'][:6]}...{node_data['id'][-6:]}\n" \
                   f"IP: {node_data['host']}\n" \
                   f"Port: {node_data['publicPort']}\n" \
                   f"State: {node_state}```"
        elif node_data["id"] is None:
            return f"{field_symbol} **NODE**\n" \
                   f"```\n" \
                   f"IP: {node_data['host']}\n" \
                   f"Port: {node_data['publicPort']}\n" \
                   f"State: {node_state}```"

    if node_data["state"] != "offline":
        field_symbol = ":green_square:"
        node_state = node_data['state'].title()
        return node_state_field()
    elif node_data["state"] == "offline":
        field_symbol = f":red_square:"
        node_state = "Offline"
        return node_state_field()

def build_general_cluster_state(node_data):
    if node_data["clusterConnectivity"] == "new association":
        return f":green_square: **CLUSTER**\n" \
               f"```Recently associated with:\n" \
               f"\"{str(node_data['clusterNames']).title()}\"```"
    elif node_data["clusterConnectivity"] == "associated":
        return f":green_square: **CLUSTER**\n" \
               f"```Associated with:\n" \
               f"\"{str(node_data['clusterNames']).title()}\"```"
    elif node_data["clusterConnectivity"] == "new dissociation":
        return f":red_square: **CLUSTER**\n" \
               f"```Recently dissociated from:\n" \
               f"\"{str(node_data['formerClusterNames']).title()}\"```"
    elif node_data["clusterConnectivity"] == "dissociated":
        return f":red_square: **CLUSTER**\n" \
               f"```Dissociated from:\n" \
               f"\"{str(node_data['formerClusterNames']).title()}\"```"
    elif node_data["clusterConnectivity"] is None:
        return f":yellow_square: **CLUSTER**\n" \
               f":information_source: No data available"

def build_general_node_wallet(node_data):
    def wallet_field(field_symbol, field_info):
        return f"{field_symbol} **WALLET**\n" \
               f"```\n" \
               f"{node_data['nodeWalletAddress']}\n" \
               f"{node_data['nodeWalletBalance']/100000000} $DAG```" \
               f"{field_info}"
    def field_from_wallet_conditions():
        if node_data["nodeWalletBalance"] >= 250000 * 100000000:
            if node_data["rewardState"] is False:
                field_symbol = ":red_square:"
                field_info = f":warning: The wallet did *not* receive rewards for the last 50 snapshots"
                return wallet_field(field_symbol, field_info)
            elif node_data["rewardState"] is True:
                field_symbol = ":green_square:"
                field_info = f":coin: The wallet receives rewards"
                return wallet_field(field_symbol, field_info)
            elif node_data["rewardState"] is None:
                field_symbol = ":yellow_square:"
                field_info = f":information_source: Unknown reward state - please report"
                return wallet_field(field_symbol, field_info)
        else:
            if (node_data["clusterNames"] or node_data["formerClusterNames"]) != "testnet":
                field_symbol = ":red_square:"
                field_info = f":warning: The wallet does *not* hold sufficient collateral"
                return wallet_field(field_symbol, field_info)
            else:
                if node_data["rewardState"] is True:
                    field_symbol = ":green_square:"
                    field_info = f":information_source: No minimum collateral required\n" \
                                 f":coin: The wallet receives rewards"
                    return wallet_field(field_symbol, field_info)
                elif node_data["rewardState"] is False:
                    field_symbol = ":red_square:"
                    field_info = f":information_source: No minimum collateral required\n" \
                                 f":warning: The wallet did *not* receive rewards for the last 50 snapshots"
                    return wallet_field(field_symbol, field_info)
                else:
                    field_symbol = ":yellow_square:"
                    field_info = f":information_source: No minimum collateral required\n" \
                                 f":information_source: The wallet reward state is unknown. Please report"
                    return wallet_field(field_symbol, field_info)


    if node_data["nodeWalletAddress"] is not None:
            return field_from_wallet_conditions()
    else:
        return f":yellow_square: **WALLET**\n" \
               f":information_source: No data available"

def build_system_node_version(node_data):
    if node_data["version"] is not None:
        if node_data["version"] == node_data["latestVersion"]:
            return f":green_square: **TESSELLATION**\n" \
                   f"```Version {node_data['version']} installed```" \
                   f":information_source: No new version available"

        elif node_data["version"] < node_data["latestVersion"]:
            return f":red_square: **TESSELLATION**\n" \
                   f"```Version {node_data['version']} installed```" \
                   f":warning: The latest version is `{node_data['latestVersion']}` - please check if an upgrade is necessary"
        elif node_data["version"] > node_data["latestVersion"]:
            return f":green_square: **TESSELLATION**\n" \
                   f"```Version {node_data['version']} installed```" \
                   f":information_source: The installed version is higher than the latest officially released version `{node_data['latestVersion']}` :fire:"
        else:
            return f":yellow_square: **TESSELLATION**\n" \
                   f"```Version {node_data['version']} installed```" \
                   f":information_source: Latest version is `{node_data['latestVersion']}`"
    else:
        return f":yellow_square: **TESSELLATION**\n" \
               f":information_source: No data available"

def build_system_node_load_average(node_data):
    def load_average_field():
        return f"{field_symbol} **CPU**\n" \
               f"```\n" \
               f"Count: {node_data['cpuCount']}\n" \
               f"Load: {node_data['1mSystemLoadAverage']}```" \
               f"{field_info}"
    if (node_data["1mSystemLoadAverage"] or node_data["cpuCount"]) is not None:
        if float(node_data["1mSystemLoadAverage"]) / float(node_data["cpuCount"]) >= 1:
            field_symbol = ":red_square:"
            field_info = f":warning: \"CPU load\" is *too high*. This value should be below \"CPU count\" ({node_data['cpuCount']}). You might need more CPU power"
            return load_average_field()
        elif float(node_data["1mSystemLoadAverage"]) / float(node_data["cpuCount"]) < 1:
            field_symbol = ":green_square:"
            field_info = f":information_source: \"CPU load\" is *OK*. This value should be below \"CPU count\" ({node_data['cpuCount']})"
            return load_average_field()
    else:
        field_symbol = ":yellow_square:"
        field_info = f":information_source: None-type is present"
        return load_average_field()


def build_embed(node_data):
    embed = nextcord.Embed(title=build_title(node_data).upper())
    embed.set_author(name=node_data["name"])
    embed.add_field(name="\u200B", value=build_general_node_state(node_data))
    embed.add_field(name=f"\u200B", value=build_general_cluster_state(node_data))
    embed.add_field(name=f"\u200B", value=build_general_node_wallet(node_data), inline=False)
    embed.add_field(name="\u200B", value=build_system_node_version(node_data), inline=False)
    embed.add_field(name="\u200B", value=build_system_node_load_average(node_data), inline=True)

    return embed

