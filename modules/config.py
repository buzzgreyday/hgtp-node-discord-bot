import aiofiles
import yaml


async def update_config_with_latest_values(cluster, configuration):

    if configuration["modules"][cluster["cluster name"]][cluster["layer"]]["id"] != cluster["id"]:
        configuration["modules"][cluster["cluster name"]][cluster["layer"]]["id"] = cluster["id"]
        async with aiofiles.open("data/config_new.yml", "w") as file:
            await file.write(yaml.dump(configuration))


async def load():
    async with aiofiles.open('data/config_new.yml', 'r') as file:
        return yaml.safe_load(await file.read())
