import asyncio

import nextcord
import logging
from datetime import datetime

from aiofiles import os

from modules import determine_module


async def init_process(bot, requester):
    if requester is not None:
        await set_active_presence(bot)


async def set_active_presence(bot):
    try:
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CHANGING PRESENCE TO \"ACTIVE\"")
        return await bot.change_presence(activity=nextcord.Activity(type=nextcord.ActivityType.watching, name=f'nodes since {datetime.utcnow().strftime("%H:%M")} UTC'), status=nextcord.Status.online)
    except Exception:
        logging.warning(f"{datetime.utcnow().strftime('%H:%M:%S')} - ATTEMPTING TO RECONNECT BEFORE CHANGING PRESENCE TO \"ACTIVE\"")
        await bot.wait_until_ready()
        await bot.connect(reconnect=True)
        return await bot.change_presence(activity=nextcord.Activity(type=nextcord.ActivityType.watching, name=f'nodes since {datetime.utcnow().strftime("%H:%M")} UTC'), status=nextcord.Status.online)


async def send_request_process_msg(ctx):
    msg = await ctx.message.author.send(
        "**`➭ 1. Request added to queue.`**\n"
        "`  2. Process request.`\n"
        "`  3. Send report(s).`"
    )
    return msg


async def update_request_process_msg(process_msg, process_num, foo):
    if process_msg is None:
        return None
    elif process_msg is not None:
        if process_num == 1:
            return await process_msg.edit("**`✓ 1. Request added to queue.`**\n"
                                          "**`➭ 2. Processing:`**\n"
                                          "**`   ➥ Preliminary data...`**\n"
                                          "`  3. Send report(s).`")
        elif process_num == 2:
            return await process_msg.edit("**`✓ 1. Request added to queue.`**\n"
                                          "**`➭ 2. Preparing:`**\n"
                                          "**`   ➥ Historic node data...`**\n"
                                          "`  3. Send report(s).`")
        elif process_num == 3:
            return await process_msg.edit("**`✓ 1. Request added to queue.`**\n"
                                          "**`➭ 2. Preparing:`**\n"
                                          "**`   ➥ API node data...`**\n"
                                          "`  3. Send report(s).`")
        elif process_num == 4:
            return await process_msg.edit("**`✓ 1. Request added to queue.`**\n"
                                          "**`➭ 2. Preparing:`**\n"
                                          f"**`   ➥ {foo.title()} node data...`**\n"
                                          "`  3. Send report(s).`")
        elif process_num == 5:
            return await process_msg.edit("**`✓ 1. Request added to queue.`**\n"
                                          "**`➭ 2. Processing:`**\n"
                                          "**`   ➥ Building report(s)...`**\n"
                                          "`  3. Send report(s).`")
        elif process_num == 6:
            return await process_msg.edit("**`✓ 1. Request added to queue.`**\n"
                                          "**`✓ 2. Processing:`**\n"
                                          "**`   ➥ Done!`**\n"
                                          "**`➭ 3. Sending report(s).`**")
        elif process_num == 7:
            return await process_msg.edit("**`✓ 1. Request added to queue.`**\n"
                                          "**`✓ 2. Processing:`**\n"
                                          "**`   ➥ Done!`**\n"
                                          "**`✓ 3. Report(s) sent.`**")


async def get_requester(ctx):
    return ctx.message.author.id


async def send(ctx, process_msg, bot, data, configuration):
    futures = []
    for node_data in data:
        if node_data["notify"] is True:
            if node_data["state"] != "offline":
                cluster_name = node_data["clusterNames"]
            else:
                cluster_name = node_data["formerClusterNames"]
            if await os.path.exists(f"{configuration['file settings']['locations']['cluster modules']}/{cluster_name}.py"):
                module = determine_module.set_module(cluster_name, configuration)
                embed = module.build_embed(node_data)
                if process_msg is not None:
                    futures.append((asyncio.create_task(ctx.author.send(embed=embed))))
                elif process_msg is None:
                    futures.append(asyncio.create_task(bot.get_channel(977357753947402281).send(embed=embed)))
    for fut in futures:
        await fut

