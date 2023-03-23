async def locate_id_offline(cluster_layer, cluster_name, configuration):
    return configuration["source ids"][cluster_layer][cluster_name]


async def update_config_with_latest_values(cluster, configuration):
    import yaml
    import aiofiles
    for layer in configuration["source ids"]:
        if layer == cluster["layer"]:
            for cluster_name in configuration["source ids"][layer]:
                if cluster_name == cluster["cluster name"]:
                    if configuration["source ids"][layer][cluster_name] != cluster["id"]:
                        configuration["source ids"][layer][cluster_name] = cluster["id"]
                        async with aiofiles.open("data/config.yml", "w") as file:
                            await file.write(yaml.dump(configuration))
    del layer