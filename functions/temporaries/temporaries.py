from functions.clusters import mainnet

async def run(node_data, all_supported_clusters_data, configuration):
    node_data = await mainnet.reward_check(node_data, all_supported_clusters_data)
    return node_data