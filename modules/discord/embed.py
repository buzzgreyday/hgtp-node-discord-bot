import nextcord


def build_title(node_data):
    title_state = None
    # TITLE LAYER
    if node_data["layer"] == 0:
        title_layer = f"{node_data['host']} layer 0"
    else:
        title_layer = f"{node_data['host']} layer 1"

    # TITLE STATE
    if node_data["clusterConnectivity"] in ("new association", "associated"):
        title_state = "up"
    elif node_data["clusterConnectivity"] in ("new dissociation", "dissociated"):
        title_state = "down"
    elif node_data["clusterConnectivity"] is None:
        title_state = node_data["state"]
    # TITLE
    return f"HGTP NODE REPORT\n" \
           f"{title_layer} is {title_state}".upper()

def build_general(node_data):
    if node_data["state"] != "offline" and node_data['id'] is not None:
        general_node_state = f"**Node**\n" \
                             f":green_square: `Ip: {node_data['host']}`\n" \
                             f"`   Port: {node_data['publicPort']}`\n" \
                             f"`   Id: {node_data['id'][:6]}...{node_data['id'][-6:]}`"
    elif node_data["state"] != "offline" and node_data['id'] is None:
        general_node_state = f"**Node**\n" \
                             f":green_square: `Ip: {node_data['host']}`\n" \
                             f"`   Port: {node_data['publicPort']}`"
    elif node_data["state"] == "offline" and node_data['id'] is not None:
        general_node_state = f"**Node**\n" \
                             f":red_square: `Ip: {node_data['host']}`\n" \
                             f"`   Port: {node_data['publicPort']}`\n" \
                             f"`   Id: {node_data['id'][:6]}...{node_data['id'][-6:]}`"
    elif node_data["state"] == "offline" and node_data['id'] is None:
        general_node_state = f"**Node**\n" \
                             f":red_square: `Ip: {node_data['host']}`\n" \
                             f"`   Port: {node_data['publicPort']}`"

    if node_data["clusterConnectivity"] in ("new association", "associated"):
        general_cluster_connectivity = f"**Cluster connectivity**\n" \
                                       f":green_square: {node_data['clusterConnectivity']}"
    elif node_data["clusterConnectivity"] in ("new dissociation", "dissociated"):
        general_cluster_connectivity = f"**Cluster connectivity**\n" \
                                       f":red_square: {node_data['clusterConnectivity']}"
    else:
        general_cluster_connectivity = f"**Cluster connectivity**\n" \
                                       f":yellow_square: {node_data['clusterConnectivity']}"

    if node_data["clusterState"] != "offline" and node_data["clusterState"] is not None:
        general_cluster_state = f"**Cluster**\n" \
                                f":green_square: {node_data['clusterNames']}"
    elif node_data["clusterState"] != "offline" and node_data["clusterState"] is None:
        general_cluster_state = f"**Cluster**\n" \
                                f":yellow_square: {node_data['clusterNames']}"
    else:
        general_cluster_state = f"**Cluster**\n" \
                                f":red_square: {node_data['clusterNames']}"
    return general_node_state, general_cluster_state, general_cluster_connectivity

    # REMEMBER CLUSTER/MODULE SPECIFIC ENTRIES

def build_embed(node_data):
    title = build_title(node_data)
    general_node_state, general_cluster_state, general_cluster_connectivity = build_general(node_data)
    embed = nextcord.Embed(title=title)
    embed.add_field(name="GENERAL", value=general_node_state)
    embed.add_field(name=f"\u200B", value=general_cluster_state)
    embed.add_field(name=f"\u200B", value=general_cluster_connectivity)
    return embed