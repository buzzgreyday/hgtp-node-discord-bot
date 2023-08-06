import asyncio
import random
from typing import List

import logging

from aiofiles import os
import nextcord

from assets.src import schemas, determine_module
from assets.src.discord import defaults


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
                f"```{invalid}````Please make sure the IP and port is correct and the node is online. You can subscribe the correct IP(s) and port(s) when the node services are online`"
            )
        else:
            return await process_msg.edit(
                "**`✓  1. Add subscription request to queue`**\n"
                "**`✓  2. Gather information`**\n"
                "**`✓  3. Subscribe`**"
            )


async def send_request_process_msg(ctx):
    msg = await ctx.message.author.send(
        "**`✓ 1. Add report request to queue`**\n"
        "**`➭ 2. Process data`**\n"
        "`  3. Report`"
    )
    return msg


async def return_guild_member_role(bot, ctx):
    guild = await bot.fetch_guild(974431346850140201)
    member = await guild.fetch_member(ctx.author.id)
    role = nextcord.utils.get(guild.roles, name="tester")
    return guild, member, role


async def track_reactions(ctx, bot):
    timeout = 60
    def check(reaction, user):
        return user != bot.user and reaction.message.id == verify_msg.id
    greetings = ["Hi", "Hallo","Greetings", "Hey"]
    introduction = ["As you might have noticed, I did some checking and you're DMs are open. Ready to claim your role as a`verified`member? :robot:",
                    "As you might have noticed, I did some banging on the pipes and you're elible for the role as a `verified` member :robot:",
                    "Good news! As you might have noticed, I was able to reach you through DM. Therefore you're able to claim the role as a verified member"]
    verify_msg = await ctx.channel.send(
        f"{random.choice(greetings)}, {ctx.message.author.mention}.\n"
        f"{random.choice(introduction)}\n"
        f"Please react to this message with an optional emoji to gain the `verified` role.\n\n"
        f"`This message will burn in {timeout} seconds`")
    await bot.wait_for("reaction_add", check=check, timeout=timeout)  # Adjust the timeout as needed
    guild = await bot.fetch_guild(974431346850140201)
    role = nextcord.utils.get(guild.roles, name="verified")
    await ctx.message.author.add_roles(role)
    await ctx.message.delete()
    await verify_msg.delete()
    confirm_msg = await ctx.channel.send(f"Dear {ctx.message.author.mention} :heart:\n"
                                         f"I assigned you the role as a `verified` member. You're now able to subscribe node(s).\n"
                                         "See how to subscribe your node(s) here:\n"
                                         "> <#993895415873273916>\n"
                                         "All commands can also be used by DMing the Node Robot:\n"
                                         "> <#977302927154769971>\n\n"
                                         f"`This message will burn in {timeout} seconds`")
    await asyncio.sleep(timeout)
    await confirm_msg.delete()
    logging.getLogger(__name__).info(f"discord.py - Verification of {ctx.message.author} accepted, granted role")

async def verification_denied(ctx):
    timeout = 60
    await ctx.message.delete()
    msg = await ctx.channel.send(f"Hi {ctx.message.author.mention},\n"
                                 f"Please allow me to DM you. Otherwise, I can't grant you the `verified` user privileges:\n"
                                 "> * Click the server title at the top of the left menu\n"
                                 "> * Go to `Privacy Settings`\n"
                                 "> * Enable/allow `Direct Messages`\n"
                                 "> * Come back here and write me an electronic message\n"
                                 "If you're having trouble write <@794353079825727500> a letter.\n\n"
                                 "`This message will burn in 60 seconds`")
    await asyncio.sleep(timeout)
    await msg.delete()


async def update_request_process_msg(process_msg, process_num, foo):
    if process_msg is None:
        return None
    elif process_msg is not None:
        if process_num == 1:
            return await process_msg.edit("**`✓ 1. Add report request to queue`**\n"
                                          "**`➭ 2. Process data`**\n"
                                          "`  >  Current cluster data`\n"
                                          "`  *  Historic node data`\n"
                                          "`  *  Current node data`\n"
                                          f"`  *  Process aggregated data`\n"
                                          "`  3. Report`\n"
                                          "`  *  Build report(s)`\n"
                                          "`  *  Send report(s)`\n")
        elif process_num == 2:
            return await process_msg.edit("**`✓ 1. Add report request to queue`**\n"
                                          "**`➭ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "`  >  Historic node data`\n"
                                          "`  *  Current node data`\n"
                                          f"`  *  Process aggregated data`\n"
                                          "`  3. Report`\n"
                                          "`  *  Build report(s)`\n"
                                          "`  *  Send report(s)`\n")
        elif process_num == 3:
            return await process_msg.edit("**`✓ 1. Add report request to queue`**\n"
                                          "**`➭ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "**`  ✓  Historic node data`**\n"
                                          "`  >  Current node data`\n"
                                          f"`  *  Process aggregated data`\n"
                                          "`  3. Report`\n"
                                          "`  *  Build report(s)`\n"
                                          "`  *  Send report(s)`\n")
        elif process_num == 4:
            return await process_msg.edit("**`✓ 1. Add report request to queue`**\n"
                                          "**`➭ 2. Process data`**\n"
                                          "**`  ✓  Current cluster data`**\n"
                                          "**`  ✓  Historic node data`**\n"
                                          "**`  ✓  Current node data`**\n"
                                          f"`  >  Processing {foo.title()} data`\n"
                                          "`  3. Report`\n"
                                          "`  *  Build report(s)`\n"
                                          "`  *  Send report(s)`\n")
        elif process_num == 5:
            return await process_msg.edit("**`✓ 1. Add report request to queue`**\n"
                                          "**`✓ 2. Process data`**\n"
                                          "**`➭ 3. Report`**\n"
                                          "`  >  Build report(s)`\n"
                                          "`  *  Send report(s)`\n")
        elif process_num == 6:
            return await process_msg.edit("**`✓ 1. Add report request to queue`**\n"
                                          "**`✓ 2. Process data`**\n"
                                          "**`➭ 3. Report`**\n"
                                          "**`  ✓  Build report(s)`**\n"
                                          "`  >  Send report(s)`\n")
        elif process_num == 7:
            return await process_msg.edit("**`✓ 1. Add report request to queue`**\n"
                                          "**`✓ 2. Process data`**\n"
                                          "**`✓ 3. Report`**\n")


async def role_deny_request_update_process_msg(process_msg):
    return await process_msg.edit(
        "**`➭ 1. Add report request to queue`**\n"
        "**`  X  You're not a subscriber`**\n"
        "*`     Please subscribe to request reports`*\n"
        "`  2. Process data`\n"
        "`  3. Report`"
    )


async def get_requester(ctx):
    return ctx.message.author.id


async def send(ctx, process_msg, bot, data: List[schemas.Node], configuration):
    logging.getLogger(__name__).info(f"discord.py - Preparing {len(data)} reports")
    futures = []
    for node_data in data:
        if node_data.notify is True:
            module_name = list(str(value) for value in (node_data.cluster_name, node_data.former_cluster_name, node_data.last_known_cluster_name) if value is not None)
            if module_name:
                module_name = module_name[0]
            else:
                module_name = None
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
                logging.getLogger(__name__).debug(f"discord.py - Node report successfully sent to {node_data.name} ({node_data.ip}, L{node_data.layer}):\n\t{node_data}")
            elif process_msg is None:
                guild = await bot.fetch_guild(974431346850140201)
                member = await guild.fetch_member(int(node_data.contact))
                futures.append(asyncio.create_task(member.send(embed=embed)))
                logging.getLogger(__name__).debug(f"discord.py - Node report successfully sent to {node_data.name} ({node_data.ip}, L{node_data.layer}):\n\t{node_data}")

    for fut in futures:
        await fut

