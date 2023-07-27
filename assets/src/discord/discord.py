import asyncio
from typing import List

import nextcord
import logging
from datetime import datetime

from aiofiles import os

from assets.src import schemas, determine_module
from assets.src.discord import defaults


async def send_request_process_msg(ctx):
    msg = await ctx.message.author.send(
        "**`➭ 1. Add request to queue`**\n"
        "`  2. Process data`\n"
        "`  3. Report`"
    )
    return msg


async def update_request_process_msg(process_msg, process_num, foo):
    if process_msg is None:
        return None
    elif process_msg is not None:
        if process_num == 1:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`➭ 2. Process data`**\n"
                                          "`  >  Current cluster data`\n"
                                          "`  *  Historic node data`\n"
                                          "`  *  Current node data`\n"
                                          f"`  *  Process aggregated data`\n"
                                          "`  3. Report`\n"
                                          "`  *  Build report(s)`\n"
                                          "`  *  Send report(s)`\n")
        elif process_num == 2:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`➭ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "`  >  Historic node data`\n"
                                          "`  *  Current node data`\n"
                                          f"`  *  Process aggregated data`\n"
                                          "`  3. Report`\n"
                                          "`  *  Build report(s)`\n"
                                          "`  *  Send report(s)`\n")
        elif process_num == 3:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`➭ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "**`  ✓  Historic node data`**\n"
                                          "`  >  Current node data`\n"
                                          f"`  *  Process aggregated data`\n"
                                          "`  3. Report`\n"
                                          "`  *  Build report(s)`\n"
                                          "`  *  Send report(s)`\n")
        elif process_num == 4:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`➭ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "**`  ✓  Historic node data`**\n"
                                          "**`  ✓  Current node data`**\n"
                                          f"`  >  Now processing {foo.title()} data`\n"
                                          "`  3. Report`\n"
                                          "`  *  Build report(s)`\n"
                                          "`  *  Send report(s)`\n")
        elif process_num == 5:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`✓ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "**`  ✓  Historic node data`**\n"
                                          "**`  ✓  Current node data`**\n"
                                          "**`  ✓  All data processed`**\n"
                                          "**`➭ 3. Report`**\n"
                                          "`  >  Build report(s)`\n"
                                          "`  *  Send report(s)`\n")
        elif process_num == 6:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`✓ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "**`  ✓  Historic node data`**\n"
                                          "**`  ✓  Current node data`**\n"
                                          "**`  ✓  All data processed`**\n"
                                          "**`➭ 3. Report`**\n"
                                          "**`  ✓  Build report(s)`**\n"
                                          "`  >  Send report(s)`\n")
        elif process_num == 7:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`✓ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "**`  ✓  Historic node data`**\n"
                                          "**`  ✓  Current node data`**\n"
                                          "**`  ✓  All data processed`**\n"
                                          "**`✓ 3. Report`**\n"
                                          "**`  ✓  Build report(s)`**\n"
                                          "**`  ✓  Send report(s)`**\n")


async def get_requester(ctx):
    return ctx.message.author.id


async def send(ctx, process_msg, bot, data: List[schemas.Node], configuration):
    logging.getLogger(__name__).info(f"discord.py - Sending node {len(data)} reports")
    futures = []
    for node_data in data:
        if node_data.notify is True:
            module_name = list(str(value) for value in (node_data.cluster_name, node_data.former_cluster_name, node_data.last_known_cluster_name) if value is not None)[0]
            if await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{module_name}.py"):
                logging.getLogger(__name__).debug(f"discord.py - Choosing {module_name} module embed type for {node_data.name} ({node_data.ip}, L{node_data.layer})")
                module = determine_module.set_module(module_name, configuration)
                embed = module.build_embed(node_data, module_name)
            else:
                logging.getLogger(__name__).debug(f"discord.py - Choosing default embed type for {node_data.name} ({node_data.ip}, L{node_data.layer})")
                embed = defaults.build_embed(node_data)
            if process_msg is not None:
                logging.getLogger(__name__).debug(f"discord.py - Sending node report to {node_data.name} ({node_data.ip}, L{node_data.layer})")
                futures.append((asyncio.create_task(ctx.author.send(embed=embed))))
                logging.getLogger(__name__).debug(f"discord.py - Node report successfully sent to {node_data.name} ({node_data.ip}, L{node_data.layer})")
            elif process_msg is None:
                logging.getLogger(__name__).debug(f"discord.py - Node report successfully sent to {node_data.name} ({node_data.ip}, L{node_data.layer})")
                futures.append(asyncio.create_task(bot.get_channel(977357753947402281).send(embed=embed)))

    for fut in futures:
        await fut

