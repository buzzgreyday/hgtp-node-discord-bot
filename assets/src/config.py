import aiofiles
import yaml

from assets.src import schemas


async def update_config_with_latest_values(cluster: schemas.Cluster, configuration):

    if configuration["modules"][cluster.name][cluster.layer]["id"] != cluster.id:
        configuration["modules"][cluster.name][cluster.layer]["id"] = cluster.id
        async with aiofiles.open("config.yml", "w") as file:
            await file.write(yaml.dump(configuration))


async def load():
    async with aiofiles.open('config.yml', 'r') as file:
        return yaml.safe_load(await file.read())
