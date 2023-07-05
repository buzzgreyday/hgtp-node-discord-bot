import importlib.util
from typing import List

from aiofiles import os
import sys

from assets.src import schemas


async def notify(data: List[schemas.Node], configuration):
    for idx, d in enumerate(data):
        if await os.path.exists(
                f"{configuration['file settings']['locations']['cluster modules']}/{d.cluster_name}.py"):
            module = set_module(d.cluster_name, configuration)
            data[idx] = module.mark_notify(d, configuration)
    return data


def set_module(cluster_name, configuration):
    spec = importlib.util.spec_from_file_location(f"{cluster_name}.build_embed",
                                                  f"{configuration['file settings']['locations']['cluster modules']}/{cluster_name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[f"{cluster_name}.build_embed"] = module
    spec.loader.exec_module(module)
    return module