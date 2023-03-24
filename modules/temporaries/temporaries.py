from modules.clusters import mainnet

async def run(node_data, all_supported_clusters_data):
    return await mainnet.reward_check(node_data, all_supported_clusters_data)