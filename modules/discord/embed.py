import nextcord


def build_title(node_data):
    title_layer = None
    title_state = None
    # TITLE CLUSTER
    if node_data["clusterNames"] is not None:
        title_cluster = node_data["clusterNames"]
    elif node_data["clusterNames"] is None and node_data["formerClusterNames"] is not None:
        title_cluster = node_data["formerClusterNames"]
    else:
        title_cluster = None
    # TITLE LAYER
    if node_data["layer"] == 0:
        if title_cluster is not None:
            title_layer = f"{title_cluster} validator layer"
        else:
            title_layer = f"Validator layer"
    elif node_data["layer"] == 1:
        if title_cluster is not None:
            title_layer = f"{title_cluster} metagraph layer"
        else:
            title_layer = f"Metagraph layer"
    # TITLE STATE
    if node_data["clusterConnectivity"] in ("new association", "associated"):
        title_state = "up"
    elif node_data["clusterConnectivity"] in ("new dissociation", "dissociated"):
        title_state = "down"
    elif node_data["clusterConnectivity"] is None:
        title_state = node_data["state"]
    # TITLE
    return f"CONSTELLATION NODE REPORT\n" \
           f"{title_layer} is {title_state}"

def build_general(node_data):
    if node_data["state"] != "offline" and node_data['id'] is not None:
        general_node_state = f":green_square: Ip: {node_data['host']} Port: {node_data['publicPort']} Id: {node_data['id'][:6]}...{node_data['id'][-6:]}"
    elif node_data["state"] != "offline" and node_data['id'] is None:
        general_node_state = f":red_square: Ip: {node_data['host']} Port: {node_data['publicPort']}"
    elif node_data["state"] == "offline" and node_data['id'] is not None:
        general_node_state = f":red_square: Ip: {node_data['host']} Port: {node_data['publicPort']} Id: {node_data['id'][:6]}...{node_data['id'][-6:]}"
    elif node_data["state"] == "offline" and node_data['id'] is None:
        general_node_state = f":red_square: Ip: {node_data['host']} Port: {node_data['publicPort']}"

    if node_data["clusterConnectivity"] in ("new association", "associated"):
        general_cluster_connectivity = f":green_square: Connectivity: {node_data['clusterConnectivity']}"
    elif node_data["clusterConnectivity"] in ("new dissociation", "dissociated"):
        general_cluster_connectivity = f":red_square: Connectivity: {node_data['clusterConnectivity']}"
    else:
        general_cluster_connectivity = f":yellow_square: Connectivity: {node_data['clusterConnectivity']}"

    if node_data["clusterState"] != "offline" and node_data["clusterState"] is not None:
        general_cluster_state = f":green_square: Cluster: {node_data['clusterNames']}"
    elif node_data["clusterState"] != "offline" and node_data["clusterState"] is None:
        general_cluster_state = f":yellow_square: Cluster: {node_data['clusterNames']}"
    else:
        general_cluster_state = f":red_square: Cluster: {node_data['clusterNames']}"
    return f"{general_node_state}\n" \
           f"{general_cluster_state} {general_cluster_connectivity}"

    # REMEMBER CLUSTER/MODULE SPECIFIC ENTRIES

async def build_embed(contact_data):
    for node_data in contact_data:
        title = build_title(node_data)
        general = build_general(node_data)
        embed = nextcord.Embed(title=title)
        embed.add_field(name="General", value=general)
        print(f"{title}\n"
              f"{general}")