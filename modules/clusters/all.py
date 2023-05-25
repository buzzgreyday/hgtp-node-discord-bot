import yaml
import aiofiles


async def locate_id_offline(layer, name, configuration):
    return configuration["modules"][name][layer]["id"]


async def update_config_with_latest_values(cluster, configuration):

    if configuration["modules"][cluster["cluster name"]][cluster["layer"]]["id"] != cluster["id"]:
        configuration["modules"][cluster["cluster name"]][cluster["layer"]]["id"] = cluster["id"]
        async with aiofiles.open("data/config_new.yml", "w") as file:
            await file.write(yaml.dump(configuration))
