import aiofiles
import yaml


async def load():
    async with aiofiles.open('data/config_new.yml', 'r') as file:
        return yaml.safe_load(await file.read())
