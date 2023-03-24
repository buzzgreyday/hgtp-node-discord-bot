import os.path
import time
from modules import tessellation
import asyncio
import aiofiles
import importlib.util
import sys

async def supported_clusters(cluster_layer: str, cluster_names: dict, configuration: dict) -> list:

    all_clusters_data = []
    for cluster_name, cluster_info in cluster_names.items():
        for lb_url in cluster_info["url"]:
            if os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{cluster_name}.py"):
                spec = importlib.util.spec_from_file_location(f"{cluster_name}.request_cluster_data", f"{configuration['file settings']['locations']['cluster modules']}/{cluster_name}.py")
                module = importlib.util.module_from_spec(spec)
                sys.modules[f"{cluster_name}.request_cluster_data"] = module
                spec.loader.exec_module(module)
                cluster = await module.request_cluster_data(lb_url, cluster_layer, cluster_name, configuration)
                all_clusters_data.append(cluster)
    del lb_url, cluster_name, cluster_info
    return all_clusters_data

async def node_cluster(node_data, configuration):

    if os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{node_data['clusterNames']}.py"):
        spec = importlib.util.spec_from_file_location(f"{node_data['clusterNames']}.node_cluster_data",
                                                      f"{configuration['file settings']['locations']['cluster modules']}/{node_data['clusterNames']}.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[f"{node_data['clusterNames']}.node_cluster_data"] = module
        spec.loader.exec_module(module)
        node_data = await module.node_cluster_data(node_data, configuration)
        return node_data
    elif os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{node_data['formerClusterNames']}.py"):
        spec = importlib.util.spec_from_file_location(f"{node_data['formerClusterNames']}.node_cluster_data",
                                                      f"{configuration['file settings']['locations']['cluster modules']}/{node_data['formerClusterNames']}.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[f"{node_data['formerClusterNames']}.node_cluster_data"] = module
        spec.loader.exec_module(module)
        node_data = await module.node_cluster_data(node_data, configuration)
        return node_data
    else:
        return node_data

async def preliminary_data(configuration):
    import yaml
    timer_start = time.perf_counter()
    tasks = []
    cluster_data = []
    validator_mainnet_data, validator_testnet_data = await tessellation.validator_data(configuration)
    latest_tessellation_version = await tessellation.latest_version_github(configuration)
    for cluster_layer, cluster_names in list(configuration["request"]["url"]["clusters"]["load balancer"].items()):
        tasks.append(asyncio.create_task(supported_clusters(cluster_layer, cluster_names, configuration)))
    for task in tasks:
        cluster_data.append(await task)
    """RELOAD CONFIGURATION"""
    async with aiofiles.open('data/config.yml', 'r') as file:
        configuration = yaml.safe_load(await file.read())
    timer_stop = time.perf_counter()
    print("PRELIMINARIES TOOK:", timer_stop - timer_start)
    return configuration, cluster_data, validator_mainnet_data, validator_testnet_data, latest_tessellation_version

