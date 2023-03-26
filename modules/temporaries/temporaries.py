from modules.clusters import mainnet

def run(node_data, all_supported_clusters_data):
    return mainnet.reward_check(node_data, all_supported_clusters_data)