import asyncio
from typing import List

import nextcord
import logging
from datetime import datetime

from aiofiles import os

from assets.src import schemas, determine_module
from assets.src.discord import defaults


async def init_process(bot, requester):
    if requester is not None:
        await set_active_presence(bot)


async def set_active_presence(bot):
    try:
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CHANGING PRESENCE TO \"ACTIVE\"")
        return await bot.change_presence(activity=nextcord.Activity(type=nextcord.ActivityType.watching, name=f'node(s)'), status=nextcord.Status.online)
    except Exception:
        logging.warning(f"{datetime.utcnow().strftime('%H:%M:%S')} - ATTEMPTING TO RECONNECT BEFORE CHANGING PRESENCE TO \"ACTIVE\"")
        await bot.wait_until_ready()
        await bot.connect(reconnect=True)
        return await bot.change_presence(activity=nextcord.Activity(type=nextcord.ActivityType.watching, name=f'node(s)'), status=nextcord.Status.online)


async def send_request_process_msg(ctx):
    msg = await ctx.message.author.send(
        "**`➭ 1. Add request to queue`**\n"
        "`  2. Process data`\n"
        "`  3. Send report(s)`"
    )
    return msg


async def update_request_process_msg(process_msg, process_num, foo):
    if process_msg is None:
        return None
    elif process_msg is not None:
        if process_num == 1:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`➭ 2. Process data`**\n"
                                          "`  *  Current cluster data`\n"
                                          "`  3. Send report(s).`")
        elif process_num == 2:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`➭ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "`  *  Historic node data`\n"
                                          "`  3. Send report(s).`")
        elif process_num == 3:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`➭ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "**`  ✓  Historic node data`**\n"
                                          "`  *  Current node data`\n"
                                          "`  3. Send report(s).`")
        elif process_num == 4:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`➭ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "**`  ✓  Historic node data`**\n"
                                          "**`  ✓  Current node data`**\n"
                                          f"`  *  Now processing {foo.title()} node data`\n"
                                          "`  3. Send report(s).`")
        elif process_num == 5:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`✓ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "**`  ✓  Historic node data`**\n"
                                          "**`  ✓  Current node data`**\n"
                                          "**`  ✓  All data processed`**\n"
                                          "**`➭ 3. Send report(s).`**\n"
                                          "`  *  Building report(s)`\n")
        elif process_num == 6:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`✓ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "**`  ✓  Historic node data`**\n"
                                          "**`  ✓  Current node data`**\n"
                                          "**`  ✓  All data processed`**\n"
                                          "**`➭ 3. Send report(s).`**\n"
                                          "**`  ✓  Building report(s)`**\n"
                                          "`  *  Sending report(s)`\n")
        elif process_num == 7:
            return await process_msg.edit("**`✓ 1. Add request to queue`**\n"
                                          "**`✓ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "**`  ✓  Historic node data`**\n"
                                          "**`  ✓  Current node data`**\n"
                                          "**`  ✓  All data processed`**\n"
                                          "**`✓ 3. Send report(s).`**\n"
                                          "**`  ✓  Building report(s)`**\n"
                                          "**`  ✓  Report(s) sent`**\n")


async def get_requester(ctx):
    return ctx.message.author.id


async def send(ctx, process_msg, bot, data: List[schemas.Node], configuration):
    futures = []
    for node_data in data:
        if node_data.notify is True:
            module_name = list(str(value) for value in (node_data.cluster_name, node_data.former_cluster_name, node_data.last_known_cluster_name) if value is not None)[0]
            print(f"{configuration['file settings']['locations']['cluster modules']}/{module_name}.py")
            if await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{module_name}.py"):
                print(f"Choosing {module_name} module embed type.")
                module = determine_module.set_module(module_name, configuration)
                embed = module.build_embed(node_data, module_name)
            else:
                print("Choosing default embed type.")
                embed = defaults.build_embed(node_data)
            if process_msg is not None:
                futures.append((asyncio.create_task(ctx.author.send(embed=embed))))
            elif process_msg is None:
                futures.append(asyncio.create_task(bot.get_channel(977357753947402281).send(embed=embed)))

    for fut in futures:
        await fut

