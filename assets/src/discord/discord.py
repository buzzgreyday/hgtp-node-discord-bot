import asyncio
import datetime
import traceback
from typing import List

import logging

from aiofiles import os
import nextcord

from assets.src import schemas, determine_module
from assets.src.discord import defaults
from assets.src.discord.services import dev_env, NODEBOT_DEV_GUILD, NODEBOT_GUILD


async def send_subscription_process_msg(ctx):
    msg = await ctx.message.author.send(
        "**`➭ 1. Add subscription request to queue`**\n"
        "`  2. Gather information`\n"
        "`  3. Subscribe`"
    )
    return msg


async def deny_subscription(process_msg):
    return await process_msg.edit(
        "**`✓  1. Add subscription request to queue`**\n"
        "**`✓  2. Gather information`**\n"
        "**`➭  3. Subscribe`**\n"
        "`   X  Subscription denied`\n"
        "**:warning:` We could not verify the IP as belonging to a node or the port(s) are not open or not correct`**"
    )


async def update_subscription_process_msg(process_msg, process_num, foo):
    if process_num == 1:
        return await process_msg.edit(
            "**`✓  1. Add subscription request to queue`**\n"
            "**`➭  2. Gather information`**\n"
            "`   *  Process data`\n"
            "`   3. Subscribe`\n"
            "`   *  Write to database and assign role`\n"
        )
    elif process_num == 2:
        return await process_msg.edit(
            "**`✓  1. Add subscription request to queue`**\n"
            "**`➭  2. Gather information`**\n"
            f"`   >  Now processing {foo}`\n"
            "`   3. Subscribe`\n"
            "`   *  Write to database and assign role`\n"
        )
    elif process_num == 3:
        return await process_msg.edit(
            "**`✓  1. Add subscription request to queue`**\n"
            "**`✓  2. Gather information`**\n"
            "**`➭  3. Subscribe`**\n"
            "`    >  Write to database and assign role`\n"
        )
    elif process_num == 4:
        invalid = list(f"IP: {val[0]} Port: {val[1]}" for val in foo)
        if foo:
            return await process_msg.edit(
                "**`✓  1. Add subscription request to queue`**\n"
                "**`✓  2. Gather information`**\n"
                "**`✓  3. Subscribe`**\n"
                "**:warning:` The following could not be subscribed:`**"
                f"```{invalid}````Please make sure the IP and port is correct and the node is online. You can "
                f"subscribe the correct IP(s) and port(s) when the node services are online`"
            )
        else:
            return await process_msg.edit(
                "**`✓  1. Add subscription request to queue`**\n"
                "**`✓  2. Gather information`**\n"
                "**`✓  3. Subscribe`**"
            )


async def return_guild_member_role(bot, ctx):
    guild = await bot.fetch_guild(NODEBOT_DEV_GUILD if dev_env else NODEBOT_GUILD)
    member = await guild.fetch_member(ctx.author.id)
    role = nextcord.utils.get(guild.roles, name="tester")
    return guild, member, role


async def delete_message(ctx, sleep=2):
    await asyncio.sleep(sleep)
    await ctx.message.delete()


async def get_requester(ctx):
    return ctx.message.author.id


async def send(bot, node_data: schemas.Node, configuration):
    async def finalize(embed):
        try:
            await bot.wait_until_ready()
            if not dev_env:
                try:
                    member = await guild.fetch_member(int(node_data.discord))
                except TypeError as e:
                    logging.getLogger("nextcord").warning(
                        f"No discord_id present: might be due to node being offline for too long")
                else:
                    embed.set_footer(
                        text=f"Data: {node_data.timestamp_index.now(datetime.UTC).strftime('%d-%m-%Y %H:%M')} UTC\n"
                             f"Build: {configuration['general']['version']}",
                        icon_url="https://raw.githubusercontent.com/pypergraph/hgtp-node-discord-bot/master/assets/src/images"
                                 "/logo-encased-color.png",
                    )
                    await member.send(embed=embed)
                    logging.getLogger("nextcord").info(
                        f"discord.py - Node report successfully sent to {node_data.name} ({node_data.ip}, L{node_data.layer}):"
                        f"\n\t{node_data}"
                    )
            else:
                member = await guild.fetch_member(794353079825727500)
                embed.set_footer(
                    text=f"Data: {node_data.timestamp_index.now(datetime.UTC).strftime('%d-%m-%Y %H:%M')} UTC\n"
                         f"Build: {configuration['general']['version']} Development",
                    icon_url="https://raw.githubusercontent.com/pypergraph/hgtp-node-discord-bot/master/assets/src/images"
                             "/logo-encased-color.png",
                )
                await member.send(embed=embed)
                logging.getLogger("nextcord").info(
                    f"discord.py - Node report successfully sent to {node_data.name} ({node_data.ip}, L{node_data.layer}):"
                    f"\n\t{node_data}"
                )
        except nextcord.Forbidden:
            logging.getLogger("nextcord").warning(
                f"discord.py - Discord message could not be sent to "
                f"{node_data.name, node_data.ip, node_data.public_port}. The member doesn't allow DMs."
            )
        except Exception:
            logging.getLogger("nextcord").error(
                f"discord.py - Discord message could not be sent to "
                f"{node_data.name, node_data.ip, node_data.public_port}: {traceback.format_exc()}"
            )

    while True:
        try:
            guild = await bot.fetch_guild(NODEBOT_DEV_GUILD if dev_env else NODEBOT_GUILD)
        except Exception:
            logging.getLogger("nextcord").error(f"discord.py - error: {traceback.format_exc()}")
            await asyncio.sleep(3)
        else:
            break
    module_name = [value for value in (node_data.cluster_name,
                                       node_data.former_cluster_name,
                                       node_data.last_known_cluster_name)
                   if value]
    if module_name:
        module_name = module_name[0]
        if await os.path.exists(
                f"{configuration['file settings']['locations']['cluster modules']}/{module_name}.py"
        ):
            logging.getLogger("nextcord").info(
                f"discord.py - Choosing {module_name} module embed type for "
                f"{node_data.name} ({node_data.ip}, L{node_data.layer})"
            )
            module = determine_module.set_module(module_name, configuration)
            embed = module.build_embed(node_data, module_name)
            await finalize(embed)
    else:
        logging.getLogger("nextcord").info(
            f"discord.py - Choosing default embed type for {node_data.name} ({node_data.ip}, L{node_data.layer})"
        )
        embed = defaults.build_embed(node_data)

        await finalize(embed)


async def send_notification(bot, data: List[schemas.Node], configuration):
    if data:
        for node_data in data:
            if node_data.discord and node_data.notify:
                await send(bot, node_data, configuration)
