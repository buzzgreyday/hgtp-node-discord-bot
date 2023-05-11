import nextcord
import logging
from datetime import datetime


async def init_process(bot, process_msg):
    if process_msg is None:
        await set_active_presence(bot)
        return None
    elif process_msg is not None:
        return await process_msg.edit("**1. Request added to queue.**\n"
                                      "**2. Processing started. Please wait...**\n"
                                      "3. Send report(s).")


async def set_active_presence(bot):
    try:
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CHANGING PRESENCE TO \"ACTIVE\"")
        return await bot.change_presence(activity=nextcord.Activity(type=nextcord.ActivityType.watching, name=f'nodes since {datetime.utcnow().strftime("%H:%M")} UTC'), status=nextcord.Status.online)
    except Exception:
        logging.warning(f"{datetime.utcnow().strftime('%H:%M:%S')} - ATTEMPTING TO RECONNECT BEFORE CHANGING PRESENCE TO \"ACTIVE\"")
        await bot.wait_until_ready()
        await bot.connect(reconnect=True)
        return await bot.change_presence(activity=nextcord.Activity(type=nextcord.ActivityType.watching, name=f'nodes since {datetime.utcnow().strftime("%H:%M")} UTC'), status=nextcord.Status.online)


async def send_process_msg(ctx):
    msg = await ctx.message.author.send(
        "**`➡ 1. Request added to queue.`**\n"
        "`  2. Process request.`\n"
        "`  3. Send report(s).`"
    )
    return msg


async def update_proces_msg(process_msg, process_num):
    if process_msg is None:
        return None
    elif process_msg is not None:
        if process_num == 1:
            return await process_msg.edit("**`✓ 1. Request added to queue.`**\n"
                                          "**`➡ 2. Processing:`**\n"
                                          "**`   ➥ Historic node data...`**\n"
                                          "`  3. Send report(s).`")
        elif process_num == 2:
            return await process_msg.edit("**`✓ 1. Request added to queue.`**\n"
                                          "**`➭ 2. Processing:`**\n"
                                          "**`   ➥ API node data...`**\n"
                                          "`  3. Send report(s).`")
        elif process_num == 3:
            return await process_msg.edit("**`✓ 1. Request added to queue.`**\n"
                                          "**`➡ 2. Processing:`**\n"
                                          "**`   ➥ Building report(s)...`**\n"
                                          "`  3. Send report(s).`")
        elif process_num == 4:
            return await process_msg.edit("**`✓ 1. Request added to queue.`**\n"
                                          "**`✓ 2. Processing:`**\n"
                                          "**`   ➥ Done!`**\n"
                                          "**`➡ 3. Sending report(s).`**")
        elif process_num == 5:
            return await process_msg.edit("**`✓ 1. Request added to queue.`**\n"
                                          "**`✓ 2. Processing:`**\n"
                                          "**`   ➥ Done!`**\n"
                                          "**`✓ 3. Report(s) sent.`**")


async def get_requester(ctx):
    return ctx.message.author

