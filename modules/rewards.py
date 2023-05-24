from modules import determine_module


async def check(node_data, configuration):
    module = determine_module.set_module(node_data['clusterNames'], configuration)
    node_data = await module.check_rewards(node_data, configuration)
    return node_data
