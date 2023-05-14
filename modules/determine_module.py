import importlib.util
from aiofiles import os
import sys


async def notify(data, configuration):
    for i, d in enumerate(data):
        if await os.path.exists(
                f"{configuration['file settings']['locations']['cluster modules']}/{d['clusterNames']}.py"):
            module = set_module(d['clusterNames'], configuration)
            data[i] = module.mark_notify(d, configuration)
    return data


def set_module(cluster_name, configuration):
    spec = importlib.util.spec_from_file_location(f"{cluster_name}.build_embed",
                                                  f"{configuration['file settings']['locations']['cluster modules']}/{cluster_name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[f"{cluster_name}.build_embed"] = module
    spec.loader.exec_module(module)
    return module
