import importlib.util
import logging
from typing import List, Any

import sys

from aiofiles import os

from assets.src import schemas


async def get_module_name(d, configuration) -> Any | None:
    names = [a for a in (d.cluster_name, d.former_cluster_name, d.last_known_cluster_name) if a]
    if names:
        return names[0]
    else:
        return None


async def notify(data: List[schemas.Node], configuration) -> List[schemas.Node]:
    if data:
        for idx, d in enumerate(data):
            latest_known_cluster = await get_module_name(
                d, configuration
            )
            if latest_known_cluster:
                module = set_module(latest_known_cluster, configuration)
                data[idx] = module.mark_notify(d, configuration)
    return data


def set_module(cluster_name, configuration):
    if cluster_name:
        spec = importlib.util.spec_from_file_location(
            f"{cluster_name}.build_embed",
            f"{configuration['file settings']['locations']['cluster modules']}/{cluster_name}.py",
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[f"{cluster_name}.build_embed"] = module
        spec.loader.exec_module(module)
        return module


async def get_cluster_data_from_module(module_name: str, layer: int, configuration: dict) -> schemas.Cluster:
    url = configuration["modules"][module_name][layer]["url"][0]
    module_path = f"{configuration['file settings']['locations']['cluster modules']}/{module_name}.py"
    if await os.path.exists(module_path):
        module = set_module(module_name, configuration)
        cluster = await module.request_cluster_data(url=url, layer=layer, module_name=module_name, configuration=configuration)
        return cluster


async def get_module_data(node_data: schemas.Node, configuration):
    # Last known cluster is determined by all recent historic values
    last_known_cluster = await get_module_name(
        node_data, configuration
    )

    if last_known_cluster:
        module = set_module(last_known_cluster, configuration)
        node_data = await module.node_cluster_data(
            node_data, last_known_cluster, configuration
        )

    elif not last_known_cluster:
        logging.getLogger("app").warning(
            f"cluster.py - get_module_data\n"
            f"Layer: {node_data.layer}\n"
            f"Warning: No module found, no historic associations\n"
            f"Subscriber: {node_data.name} ({node_data.ip}, {node_data.public_port})\n"
        )
    return node_data
