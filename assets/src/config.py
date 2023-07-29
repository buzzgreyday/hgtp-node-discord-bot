import aiofiles
import yaml
from pathlib import Path

from assets.src import schemas

WORKING_DIR = f'{str(Path.home())}/bot'


async def update_config_with_latest_values(cluster: schemas.Cluster, configuration):
    """Update the config file with latest values"""
    if configuration["modules"][cluster.name][cluster.layer]["id"] != cluster.id:
        configuration["modules"][cluster.name][cluster.layer]["id"] = cluster.id
        async with aiofiles.open(f"{WORKING_DIR}/config.yml", "w") as file:
            await file.write(yaml.dump(configuration))


async def load():
    """Load configuration file"""
    async with aiofiles.open(f'{WORKING_DIR}/config.yml', 'r') as file:
        return yaml.safe_load(await file.read())
