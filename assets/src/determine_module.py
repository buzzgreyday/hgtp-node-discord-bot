import importlib.util
from typing import List
from pathlib import Path

from aiofiles import os
import sys

from assets.src import schemas


async def get_module_name_and_layer(d, configuration) -> tuple:
    paths = [f"{configuration['file settings']['locations']['cluster modules']}/{d.cluster_name}.py",
             f"{configuration['file settings']['locations']['cluster modules']}/{d.former_cluster_name}.py",
             f"{configuration['file settings']['locations']['cluster modules']}/{d.last_known_cluster_name}.py"]
    names = [d.cluster_name, d.former_cluster_name, d.last_known_cluster_name]
    name = [name for path, name in zip(paths, names) if await os.path.exists(path)]
    if name:
        return name[0], d.layer


async def notify(data: List[schemas.Node], configuration) -> List[schemas.Node]:
    if data:
        for idx, d in enumerate(data):
            latest_known_cluster, layer = await get_module_name_and_layer(d, configuration)
            if latest_known_cluster:
                module = set_module(latest_known_cluster, configuration)
                data[idx] = module.mark_notify(d, configuration)
    return data


def set_module(cluster_name, configuration):
    if cluster_name is not None:
        spec = importlib.util.spec_from_file_location(f"{cluster_name}.build_embed",
                                                      f"{configuration['file settings']['locations']['cluster modules']}/{cluster_name}.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[f"{cluster_name}.build_embed"] = module
        spec.loader.exec_module(module)
        return module
