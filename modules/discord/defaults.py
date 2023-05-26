import nextcord

yellow_color_trigger = False
red_color_trigger = False


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
        return f"{field_symbol} **NODE**\n" \
               f"```\n" \
               f"Peers: {node_data['nodePeerCount']}\n" \
               f"ID: {node_data['id'][:6]}...{node_data['id'][-6:]}\n" \
               f"IP: {node_data['host']}\n" \
               f"Subscribed Port: {node_data['publicPort']}\n" \
               f"State: {node_state}```" \
               f"{field_info}"

    if node_data["state"] != "offline":
        field_symbol = ":yellow_square:"
        field_info = f"`ⓘ  The node is not connected to any known cluster and no previous cluster data exists. Therfore the info shown is limited`"
        node_state = "Dissociated"
        yellow_color_trigger = True
        return node_state_field(), False, yellow_color_trigger
    elif node_data["state"] == "offline":
        field_symbol = f":red_square:"
        field_info = f"`ⓘ  The node is offline and no previous cluster data exists. Therefore the info shown is limited`"
        node_state = "Offline"
        red_color_trigger = True
        return node_state_field(), red_color_trigger, False


def build_system_node_version(node_data):

    def version_field():
        return f"{field_symbol} **TESSELLATION**\n" \
               f"```\n" \
               f"Version {node_data['version']} installed```" \
               f"{field_info}"

    if node_data["version"] is not None:

        if node_data["version"] == node_data["latestVersion"]:
            field_symbol = ":green_square:"
            field_info = "`ⓘ  No new version available`"
            return version_field(), False, False
        elif node_data["version"] < node_data["latestVersion"]:
            field_symbol = ":green_square:"
            field_info = f"`ⓘ  You are running the latest version but a new release ({node_data['latestVersion']}) should be available soon"
            return version_field(), False, False
        elif node_data["version"] > node_data["latestVersion"]:
            field_symbol = ":green_square:"
            field_info = f"`⚠  You seem to be running a test-release. Latest official version is {node_data['latestVersion']}`"
            return version_field(), False, False
        else:
            field_symbol = ":yellow_square:"
            field_info = f"`ⓘ  Latest version is {node_data['latestVersion']} and node version is {node_data['version']}. Please report`"
            yellow_color_trigger = True
            return version_field(), False, yellow_color_trigger
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
    if node_data["version"] is not None:
        embed.add_field(name="\u200B", value=node_version, inline=False)
    if node_data["1mSystemLoadAverage"] is not None:
        embed.add_field(name="\u200B", value=node_load, inline=True)
    if node_data["diskSpaceTotal"] is not None:
        embed.add_field(name="\u200B", value=node_disk, inline=True)

    return embed
