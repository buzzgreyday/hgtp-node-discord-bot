def snapshot(subscriber: dict, port: int, layer: int, latest_tessellation_version: str, dt_start):
    return {
        "name": subscriber["name"].values[0],
        "contact": subscriber["contact"].values[0],
        "host": subscriber["ip"],
        "layer": layer,
        "publicPort": port,
        "p2pPort": None,
        "id": None,
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
        "latestVersion": latest_tessellation_version,
        "cpuCount": None,
        "diskSpaceTotal": None,
        "diskSpaceFree": None,
        "1mSystemLoadAverage": None,
        "lastNotifiedTimestamp": None,
        "timestampIndex": dt_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "formerTimestampIndex": None

    }

async def embeds(contact_data):
    pass