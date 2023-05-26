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
from datetime import datetime, timedelta

import nextcord
import numpy

from modules.clusters import all
from modules import api, encode

MODULE = "testnet"
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


async def request_cluster_data(url, layer, name, configuration):
    cluster_resp = await api.safe_request(
        f"{url}/{configuration['modules'][name][layer]['info']['cluster']}", configuration)
    node_resp = await api.safe_request(
        f"{url}/{configuration['modules'][name][layer]['info']['node']}", configuration)
    latest_ordinal, latest_timestamp, addresses = await locate_rewarded_addresses(layer, name, configuration)

    if node_resp is None:
        cluster_state = "offline" ; cluster_id = await all.locate_id_offline(layer, name, configuration) ; cluster_session = None
        cluster_version = None ; cluster_host = None ; cluster_port = None
    else:
        cluster_state = str(node_resp['state']).lower() ; cluster_id = node_resp["id"] ; cluster_session = node_resp["clusterSession"]
        cluster_version = str(node_resp["version"]) ; cluster_host = node_resp["host"] ; cluster_port = node_resp["publicPort"]
    cluster = {
        "layer": layer,
        "cluster name": name,
        "state": cluster_state,
        "id": cluster_id,
        "host": cluster_host,
        "public port": cluster_port,
        "version": cluster_version,
        "peer count": len(cluster_resp),
        "cluster session": cluster_session,
        "latest ordinal": latest_ordinal,
        "latest ordinal timestamp": latest_timestamp,
        "recently rewarded": addresses,
        "peer data": sorted(cluster_resp, key=lambda d: d['id'])
    }
    await all.update_config_with_latest_values(cluster, configuration)
    del node_resp
    return cluster

# THE ABOVE FUNCTION ALSO REQUEST THE MOST RECENT REWARDED ADDRESSES. THIS FUNCTION LOCATES THESE ADDRESSES BY
# REQUESTING THE RELEVANT API'S.

# (!) YOU COULD MAKE 50 (MAGIC NUMBER) VARIABLE IN THE CONFIG YAML.
#     YOU MIGHT ALSO BE ABLE TO IMPROVE ON THE TRY/EXCEPT BLOCK LENGTH.


async def locate_rewarded_addresses(layer, name, configuration):
    """layer 1 doesn't have a block explorer: defaulting to 0"""
    try:
        latest_ordinal, latest_timestamp = \
            await request_snapshot(
                f"{configuration['modules'][name][0]['be']['url'][0]}/"
                f"{configuration['modules'][name][0]['be']['info']['latest snapshot']}", configuration)
        tasks = []
        for ordinal in range(latest_ordinal-50, latest_ordinal):
            tasks.append(asyncio.create_task(request_reward_addresses_per_snapshot(
                f"{configuration['modules'][name][0]['be']['url'][0]}/"
                f"global-snapshots/{ordinal}/rewards", configuration
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
    data = await api.safe_request(request_url, configuration)
    if data is not None:
        ordinal = data["data"]["ordinal"]
        try:
            timestamp = datetime.strptime(data["data"]["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            timestamp = datetime.strptime(data["data"]["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        return ordinal, timestamp
    elif data is None:
        ordinal = None
        timestamp = None
        return ordinal, timestamp


async def request_reward_addresses_per_snapshot(request_url, configuration):
    data = await api.safe_request(request_url, configuration)
    return list(data_dictionary["destination"] for data_dictionary in data["data"])

"""
    SECTION 2: INDIVIDUAL NODE DATA PROCESSING
"""
# ---------------------------------------------------------------------------------------------------------------------
# + NODE SPECIFIC FUNCTIONS AND CLASSES GOES HERE
# ---------------------------------------------------------------------------------------------------------------------

yellow_color_trigger = False
red_color_trigger = False


async def node_cluster_data(node_data: dict, configuration: dict) -> tuple[dict, dict]:
    if node_data['publicPort'] is not None:
        node_info_data = await api.safe_request(
            f"http://{node_data['host']}:{node_data['publicPort']}/"
            f"{configuration['modules']['testnet'][node_data['layer']]['info']['node']}", configuration)
        node_data["state"] = "offline" if node_info_data is None else node_info_data["state"].lower()
        # CHECK IF Public_Port has changed
        if node_info_data is not None:
            node_data["nodeClusterSession"] = node_info_data["clusterSession"]
            node_data["version"] = node_info_data["version"]
        if node_data["state"] != "offline":
            cluster_data = await api.safe_request(
                f"http://{str(node_data['host'])}:{str(node_data['publicPort'])}/"
                f"{configuration['modules']['testnet'][node_data['layer']]['info']['cluster']}", configuration)
            metrics_data = await api.safe_request(
                f"http://{str(node_data['host'])}:{str(node_data['publicPort'])}/"
                f"{configuration['modules']['testnet'][node_data['layer']]['info']['metrics']}", configuration)
            node_data["id"] = node_info_data["id"]
            node_data["nodeWalletAddress"] = encode.id_to_dag_address(node_data["id"])
            node_data["nodePeerCount"] = len(cluster_data) if cluster_data is not None else 0
            # WILL FAIL IF = NONE
            node_data.update(metrics_data)
        node_data = await request_wallet_data(node_data, configuration)
        node_data = set_connectivity_specific_node_data_values(node_data)
        node_data = set_association_time(node_data)

    return node_data


def check_rewards(node_data: dict, cluster_data):

    # if (cluster["layer"] == f"layer {node_data['layer']}") and (cluster["cluster name"] == node_data["clusterNames"]):
    # if (cluster["cluster name"] == node_data["clusterNames"]) or (cluster["cluster name"] == node_data["formerClusterNames"]):
    if str(node_data["nodeWalletAddress"]) in cluster_data["recently rewarded"]:
        node_data["rewardState"] = True
        if node_data["rewardTrueCount"] is None:
            former_reward_count = 0
        else:
            former_reward_count = node_data["rewardTrueCount"]
        node_data["rewardTrueCount"] = former_reward_count + 1
        if node_data["rewardFalseCount"] is None:
            node_data["rewardFalseCount"] = 0
    elif str(node_data["nodeWalletAddress"]) not in cluster_data["recently rewarded"]:
        node_data["rewardState"] = False
        if node_data["rewardFalseCount"] is None:
            former_reward_count = 0
        else:
            former_reward_count = node_data["rewardFalseCount"]
        node_data["rewardFalseCount"] = former_reward_count + 1
        if node_data["rewardTrueCount"] is None:
            node_data["rewardTrueCount"] = 0

    return node_data


async def request_wallet_data(node_data, configuration):

    wallet_data = await api.safe_request(f"{configuration['modules']['testnet'][0]['be']['url'][0]}/addresses/{node_data['nodeWalletAddress']}/balance", configuration)
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

    former_name = node_data["formerClusterNames"]
    curr_name = node_data["clusterNames"]
    session = node_data["nodeClusterSession"]
    latest_session = node_data["latestClusterSession"]

    if MODULE == curr_name and MODULE != former_name and session == latest_session:
        node_data["clusterConnectivity"] = "new association"
    elif former_name == curr_name and MODULE == former_name and session == latest_session:
        node_data["clusterConnectivity"] = "association"
    elif MODULE != curr_name and MODULE == former_name and session != latest_session:
        node_data["clusterConnectivity"] = "new dissociation"
    elif MODULE != curr_name and MODULE != former_name and session != latest_session:
        node_data["clusterConnectivity"] = "dissociation"

    return node_data


def set_association_time(node_data):
    if node_data["formerTimestampIndex"] is not None:
        # LINE BELOW IS TEMPORARY
        time_difference = (datetime.strptime(node_data["timestampIndex"], "%Y-%m-%dT%H:%M:%S.%fZ") - datetime.strptime(node_data["formerTimestampIndex"], "%Y-%m-%dT%H:%M:%S.%fZ")).seconds
    else:
        time_difference = datetime.strptime(node_data["timestampIndex"], "%Y-%m-%dT%H:%M:%S.%fZ").second

    for connectivity_type in ("association", "dissociation"):
        if node_data[f'cluster{connectivity_type.title()}Time'] is None:
            node_data[f'cluster{connectivity_type.title()}Time'] = 0
        if node_data[f'formerCluster{connectivity_type.title()}Time'] is None:
            node_data[f'formerCluster{connectivity_type.title()}Time'] = 0

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
    if node_data["clusterConnectivity"] in ("new association", "associated"):
        title_ending = f"is up"
    elif node_data["clusterConnectivity"] in ("new dissociation", "dissociated"):
        title_ending = f"is down"
    else:
        title_ending = f"report"
    if node_data['clusterNames'] is not None:
        return f"{node_data['clusterNames'].title()} layer {node_data['layer']} node ({node_data['host']}) {title_ending}"
    else:
        return f"layer {node_data['layer']} node ({node_data['host']}) {title_ending}"


def build_general_node_state(node_data):
    def node_state_field():
        if node_data["id"] is not None:
            return f"{field_symbol} **NODE**\n" \
                   f"```\n" \
                   f"Peers: {node_data['nodePeerCount']}\n" \
                   f"ID: {node_data['id'][:6]}...{node_data['id'][-6:]}\n" \
                   f"IP: {node_data['host']}\n" \
                   f"Subscribed Port: {node_data['publicPort']}\n" \
                   f"State: {node_state}```" \
                   f"{field_info}"
        elif node_data["id"] is None:
            return f"{field_symbol} **NODE**\n" \
                   f"```\n" \
                   f"Peers: {node_data['nodePeerCount']}\n" \
                   f"IP: {node_data['host']}\n" \
                   f"Subscribed Port: {node_data['publicPort']}\n" \
                   f"State: {node_state}```" \
                   f"{field_info}"

    if node_data["state"] != "offline":
        field_symbol = ":green_square:"
        if node_data["clusterPeerCount"] in (None, 0):
            field_info = f"`ⓘ  The node is not connected to any known cluster`"
        else:
            field_info = f"`ⓘ  Connected to {node_data['nodePeerCount']*100/node_data['clusterPeerCount']}% of the cluster peers`"
        node_state = node_data['state'].title()
        return node_state_field(), False, yellow_color_trigger
    elif node_data["state"] == "offline":
        field_symbol = f":red_square:"
        field_info = f"`ⓘ  The node is connected to 0% of the previously associated cluster`"
        node_state = "Offline"
        red_color_trigger = True
        return node_state_field(), red_color_trigger, yellow_color_trigger


def build_general_cluster_state(node_data):
    def general_cluster_state_field():
        return f"{field_symbol} **TESTNET CLUSTER**\n" \
               f"```\n" \
               f"Peers:   {node_data['clusterPeerCount']}\n" \
               f"Assoc.:  {timedelta(seconds=float(node_data['clusterAssociationTime'])).days} days {association_percent()}%\n" \
               f"Dissoc.: {timedelta(seconds=float(node_data['clusterDissociationTime'])).days} days {100.00-association_percent()}%```" \
               f"{field_info}"

    def association_percent():
        if node_data["clusterAssociationTime"] and node_data["clusterDissociationTime"] not in (0, None):
            return round(float(node_data['clusterAssociationTime'])*100/float(node_data['clusterAssociationTime'])+float(node_data['clusterDissociationTime']), 2)
        elif node_data["clusterAssociationTime"] not in (0, None) and node_data["clusterDissociationTime"] == 0:
            return round(float(node_data['clusterAssociationTime'])*100/float(node_data['clusterAssociationTime'])+float(0.0), 2)
        elif node_data["clusterAssociationTime"] in (0, None) and node_data["clusterDissociationTime"] not in (0, None):
            return round(float(node_data['clusterAssociationTime'])*100/float(0.0)+float(node_data['clusterDissociationTime']), 2)
        else:
            return 0

    if node_data["clusterConnectivity"] == "new association":
        field_symbol = ":green_square:"
        field_info = f"`ⓘ  Association with the cluster was recently established`"
        return general_cluster_state_field(), False, yellow_color_trigger
    elif node_data["clusterConnectivity"] == "association":
        field_symbol = ":green_square:"
        field_info = f"`ⓘ  The node is consecutively associated with the cluster`"
        return general_cluster_state_field(), False, yellow_color_trigger
    elif node_data["clusterConnectivity"] == "new dissociation":
        field_symbol = ":red_square:"
        field_info = f"`ⓘ  The node was recently dissociated from the cluster`"
        red_color_trigger = True
        return general_cluster_state_field(), red_color_trigger, yellow_color_trigger
    elif node_data["clusterConnectivity"] == "dissociation":
        field_symbol = ":red_square:"
        field_info = f"`ⓘ  The node is consecutively dissociated from the cluster`"
        red_color_trigger = True
        return general_cluster_state_field(), red_color_trigger, yellow_color_trigger
    elif node_data["clusterConnectivity"] is None:
        field_symbol = ":yellow_square:"
        field_info = f""
        return general_cluster_state_field(), False, yellow_color_trigger
    else:
        print(node_data["clusterConnectivity"])


def build_general_node_wallet(node_data):
    def wallet_field(field_symbol, reward_percentage, field_info):
        if node_data["layer"] == 1:
            return f"{field_symbol} **WALLET**\n" \
               f"```\n" \
               f"Address: {node_data['nodeWalletAddress']}\n" \
               f"Balance: {node_data['nodeWalletBalance']/100000000} ＄DAG```" \
               f"{field_info}"
        else:
            return f"{field_symbol} **WALLET**\n" \
                   f"```\n" \
                   f"Address: {node_data['nodeWalletAddress']}\n" \
                   f"Balance: {node_data['nodeWalletBalance']/100000000} ＄DAG\n" \
                   f"Reward frequency: {round(float(reward_percentage), 2)}%```" \
                   f"{field_info}"

    def field_from_wallet_conditions():
        if node_data["rewardTrueCount"] in (0, None):
            reward_percentage = 0
        elif node_data["rewardFalseCount"] in (0, None):
            reward_percentage = 100
        else:
            reward_percentage = float(node_data['rewardTrueCount']) * 100 / float(node_data['rewardFalseCount'])
        if node_data["nodeWalletBalance"] >= 250000 * 100000000:
            if node_data["rewardState"] is False:
                # TEMPROARY FIC SINCE MAINNET LAYER ONE DOESN'T SUPPORT REWARDS
                """if node_data["layer"] == 1:
                    field_symbol = ":green_square:"
                else:"""
                field_symbol = ":red_square:"
                if node_data["formerRewardState"] is True:
                    field_info = f":red_circle:` The wallet recently stopped receiving rewards`"
                    red_color_trigger = True
                    return wallet_field(field_symbol, reward_percentage, field_info), red_color_trigger, False
                else:
                    # TEMPROARY FIC SINCE MAINNET LAYER ONE DOESN'T SUPPORT REWARDS
                    """if node_data["layer"] == 1:
                        field_info = f"`ⓘ Layer one doesn't generate rewards. Please refer to the layer 0 report.`"
                        return wallet_field(field_symbol, reward_percentage, field_info), False, False
                    else:"""
                    field_info = f":red_circle:` The wallet doesn't receive rewards`"
                    red_color_trigger = True
                    return wallet_field(field_symbol, reward_percentage, field_info), red_color_trigger, False
            elif node_data["rewardState"] is True:
                field_symbol = ":green_square:"
                if node_data["formerRewardState"] is False:
                    field_info = f":coin:` The wallet recently started receiving rewards`"
                    return wallet_field(field_symbol, reward_percentage, field_info), False, False
                else:
                    field_info = f":coin:` The wallet receives rewards`"
                    return wallet_field(field_symbol, reward_percentage, field_info), False, False
            elif node_data["rewardState"] is None:
                field_symbol = ":yellow_square:"
                field_info = f"`ⓘ  Unknown reward state - please report`"
                yellow_color_trigger = True
                return wallet_field(field_symbol, reward_percentage, field_info), False, yellow_color_trigger
        else:
            if (node_data["clusterNames"] or node_data["formerClusterNames"]) != "testnet":
                field_symbol = ":red_square:"
                field_info = f"`⚠ The wallet doesn't hold sufficient collateral`"
                red_color_trigger = True
                return wallet_field(field_symbol, reward_percentage, field_info), red_color_trigger, False
            else:
                if node_data["rewardState"] is True:
                    field_symbol = ":green_square:"
                    if node_data["formerRewardState"] is False:
                        field_info = f":coin:` The wallet recently started receiving rewards`\n" \
                                     f"`ⓘ  No minimum collateral required`"
                        return wallet_field(field_symbol, reward_percentage, field_info), False, False
                    else:
                        field_info = f":coin:` The wallet receives rewards`\n" \
                                     f"`ⓘ  No minimum collateral required`"

                        return wallet_field(field_symbol, reward_percentage, field_info), False, False

                elif node_data["rewardState"] is False:
                    field_symbol = ":red_square:"
                    if node_data["formerRewardState"] is True:
                        field_info = f":red_circle:` The wallet recently stopped receiving rewards`\n" \
                                     f"`ⓘ  No minimum collateral required`"

                        red_color_trigger = True
                        return wallet_field(field_symbol, reward_percentage, field_info), red_color_trigger, False
                    else:
                        field_info = f":red_circle:` The wallet doesn't receive rewards`\n"\
                                     f"`ⓘ  No minimum collateral required`"

                        red_color_trigger = True
                        return wallet_field(field_symbol, reward_percentage, field_info), red_color_trigger, False
                else:
                    field_symbol = ":yellow_square:"
                    field_info = f"`ⓘ  The wallet reward state is unknown. Please report`\n" \
                                 f"`ⓘ  No minimum collateral required`"

                    yellow_color_trigger = True
                    return wallet_field(field_symbol, reward_percentage, field_info), False, yellow_color_trigger

    if node_data["nodeWalletAddress"] is not None:
        field_content, red_color_trigger, yellow_color_trigger = field_from_wallet_conditions()
        return field_content, red_color_trigger, yellow_color_trigger
    else:
        return f":yellow_square: **WALLET**\n" \
               f"`ⓘ  No data available`", False, False


def build_system_node_version(node_data):

    def version_field():
        return f"{field_symbol} **TESSELLATION**\n" \
               f"```\n" \
               f"Version {node_data['version']} installed```" \
               f"{field_info}"

    if node_data["version"] is not None:
        if node_data["version"] == node_data["clusterVersion"]:
            field_symbol = ":green_square:"
            if node_data["clusterVersion"] == node_data["latestVersion"]:
                field_info = "`ⓘ  No new version available`"
            elif node_data["clusterVersion"] < node_data["latestVersion"]:
                field_info = f"`ⓘ  You are running the latest version but a new release ({node_data['latestVersion']}) should be available soon"
            elif node_data["clusterVersion"] > node_data["latestVersion"]:
                field_info = f"`ⓘ  You seem to be associated with a cluster running a test-release. Latest official version is {node_data['latestVersion']}`"
            else:
                field_info = "`ⓘ  This line should not be seen`"
            return version_field(), red_color_trigger, False

        elif node_data["version"] < node_data["clusterVersion"]:
            field_symbol = ":red_square:"
            field_info = f"`⚠ New upgrade (v{node_data['latestVersion']}) available`"
            yellow_color_trigger = True
            return version_field(), red_color_trigger, yellow_color_trigger

        elif node_data["version"] > node_data["latestVersion"]:
            field_symbol = ":green_square:"
            if node_data["version"] == node_data["clusterVersion"]:
                field_info = f"`ⓘ  You seem to be associated with a cluster running a test-release. Latest official version is {node_data['latestVersion']}`"
            else:
                field_info = f"`ⓘ  You seem to be running a test-release. Latest official version is {node_data['latestVersion']}`"
            return version_field(), red_color_trigger, False
        else:
            field_symbol = ":yellow_square:"
            field_info = f"`ⓘ  Latest version is {node_data['latestVersion']}`"
            return version_field(), red_color_trigger, False
    else:
        return f":yellow_square: **TESSELLATION**\n" \
               f"`ⓘ  No data available`", red_color_trigger, False


def build_system_node_load_average(node_data):
    def load_average_field():
        return f"{field_symbol} **CPU**\n" \
               f"```\n" \
               f"Count: {round(float(node_data['cpuCount']))}\n" \
               f"Load:  {round(float(node_data['1mSystemLoadAverage']), 2)}```" \
               f"{field_info}"

    if (node_data["1mSystemLoadAverage"] or node_data["cpuCount"]) is not None:
        if float(node_data["1mSystemLoadAverage"]) / float(node_data["cpuCount"]) >= 1:
            field_symbol = ":red_square:"
            field_info = f"`⚠ \"CPU load\" is too high - should be below \"CPU count\". You might need more CPU power`"
            yellow_color_trigger = True
            return load_average_field(), red_color_trigger, yellow_color_trigger
        elif float(node_data["1mSystemLoadAverage"]) / float(node_data["cpuCount"]) < 1:
            field_symbol = ":green_square:"
            field_info = f"`ⓘ  \"CPU load\" is ok - should be below \"CPU count\"`"
            return load_average_field(), red_color_trigger, False
    else:
        field_symbol = ":yellow_square:"
        field_info = f"`ⓘ  None-type is present`"
        return load_average_field(), red_color_trigger, False


def build_system_node_disk_space(node_data):
    def disk_space_field():
        return f"{field_symbol} **DISK**\n" \
               f"```\n" \
               f"Free:  {round(float(node_data['diskSpaceFree'])/1073741824, 2)} GB {round(float(node_data['diskSpaceFree'])*100/float(node_data['diskSpaceTotal']), 2)}%\n" \
               f"Total: {round(float(node_data['diskSpaceTotal'])/1073741824, 2)} GB```" \
               f"{field_info}"
    if node_data['diskSpaceFree'] is not None:
        if 0 <= float(node_data['diskSpaceFree'])*100/float(node_data['diskSpaceTotal']) <= 10:
            field_symbol = ":red_square:"
            field_info = f"`⚠ Free disk space is low`"
            yellow_color_trigger = True
            return disk_space_field(), red_color_trigger, yellow_color_trigger
        else:
            field_symbol = ":green_square:"
            field_info = f"`ⓘ  Free disk space is ok`"
            return disk_space_field(), red_color_trigger, False


def build_embed(node_data):
    embed_created = False

    def determine_color_and_create_embed(yellow_color_trigger, red_color_trigger):
        title = build_title(node_data).upper()
        if yellow_color_trigger and red_color_trigger is False:
            return nextcord.Embed(title=title, colour=nextcord.Color.orange())
        elif red_color_trigger:
            return nextcord.Embed(title=title, colour=nextcord.Color.brand_red())
        else:
            return nextcord.Embed(title=title, colour=nextcord.Color.dark_green())

    node_state, red_color_trigger, yellow_color_trigger = build_general_node_state(node_data)
    if (red_color_trigger is True or yellow_color_trigger is True) and not embed_created:
        embed = determine_color_and_create_embed(yellow_color_trigger, red_color_trigger)
        embed_created = True
    cluster_state, red_color_trigger, yellow_color_trigger = build_general_cluster_state(node_data)
    if (red_color_trigger is True or yellow_color_trigger is True) and not embed_created:
        embed = determine_color_and_create_embed(yellow_color_trigger, red_color_trigger)
        embed_created = True
    if node_data["nodeWalletAddress"] is not None:
        node_wallet, red_color_trigger, yellow_color_trigger = build_general_node_wallet(node_data)
        if (red_color_trigger is True or yellow_color_trigger is True) and not embed_created:
            embed = determine_color_and_create_embed(yellow_color_trigger, red_color_trigger)
            embed_created = True
    if node_data["version"] is not None:
        node_version, red_color_trigger, yellow_color_trigger = build_system_node_version(node_data)
        if (red_color_trigger is True or yellow_color_trigger is True) and not embed_created:
            embed = determine_color_and_create_embed(yellow_color_trigger, red_color_trigger)
            embed_created = True
    if node_data["1mSystemLoadAverage"] is not None:
        node_load, red_color_trigger, yellow_color_trigger = build_system_node_load_average(node_data)
        if (red_color_trigger is True or yellow_color_trigger is True) and not embed_created:
            embed = determine_color_and_create_embed(yellow_color_trigger, red_color_trigger)
            embed_created = True
    if node_data["diskSpaceTotal"] is not None:
        node_disk, red_color_trigger, yellow_color_trigger = build_system_node_disk_space(node_data)
        if (red_color_trigger is True or yellow_color_trigger is True) and not embed_created:
            embed = determine_color_and_create_embed(yellow_color_trigger, red_color_trigger)
    if not embed_created:
        embed = determine_color_and_create_embed(yellow_color_trigger, red_color_trigger)
    embed.set_author(name=node_data["name"])
    embed.add_field(name="\u200B", value=node_state)
    embed.add_field(name=f"\u200B", value=cluster_state)
    if node_data["nodeWalletAddress"] is not None:
        embed.add_field(name=f"\u200B", value=node_wallet, inline=False)
    if node_data["version"] is not None:
        embed.add_field(name="\u200B", value=node_version, inline=False)
    if node_data["1mSystemLoadAverage"] is not None:
        embed.add_field(name="\u200B", value=node_load, inline=True)
    if node_data["diskSpaceTotal"] is not None:
        embed.add_field(name="\u200B", value=node_disk, inline=True)

    return embed


"""
    SECTION 5: NOTIFICATION CONDITIONS
"""


def mark_notify(d, configuration):
    # The hardcoded values should be adjustable in config_new.yml
    if d["clusterConnectivity"] in ["new association", "new dissociation"]:
        d["notify"] = True
        d["lastNotifiedTimestamp"] = d["timestampIndex"]
    if d["lastNotifiedTimestamp"] is not None:
        if d["rewardState"] is False and ((datetime.strptime(d["timestampIndex"], "%Y-%m-%dT%H:%M:%S.%fZ").second - datetime.strptime(d["lastNotifiedTimestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").second) >= timedelta(minutes=10).seconds):
            # THIS IS A TEMPORARY FIX SINCE MAINNET LAYER 1 DOESN'T SUPPORT REWARDS
            """if d["layer"] == 1:
                d["notify"] = False
            else:"""
            d["notify"] = True
            d["lastNotifiedTimestamp"] = d["timestampIndex"]
        elif (d["version"] != d["clusterVersion"]) and ((datetime.strptime(d["timestampIndex"], "%Y-%m-%dT%H:%M:%S.%fZ").second - datetime.strptime(d["lastNotifiedTimestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").second) >= timedelta(hours=6).seconds):
            d["notify"] = True
            d["lastNotifiedTimestamp"] = d["timestampIndex"]
        elif d['diskSpaceFree'] and d['diskSpaceTotal'] is not None:
            if (0 <= float(d['diskSpaceFree'])*100/float(d['diskSpaceTotal']) <= 10) and ((datetime.strptime(d["timestampIndex"], "%Y-%m-%dT%H:%M:%S.%fZ").second - datetime.strptime(d["lastNotifiedTimestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").second) >= timedelta(hours=6).seconds):
                d["notify"] = True
                d["lastNotifiedTimestamp"] = d["timestampIndex"]
    # IF NO FORMER DATA
    else:
        if d["rewardState"] is False:
            d["notify"] = True
            d["lastNotifiedTimestamp"] = d["timestampIndex"]
        elif d["version"] != d["clusterVersion"]:
            d["notify"] = True
            d["lastNotifiedTimestamp"] = d["timestampIndex"]
        elif 0 <= float(d['diskSpaceFree'])*100/float(d['diskSpaceTotal']) <= 10:
            d["notify"] = True
            d["lastNotifiedTimestamp"] = d["timestampIndex"]
    return d

