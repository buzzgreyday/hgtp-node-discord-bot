import random

greeting = ["Hi", "Hallo", "Greetings", "Hey"]
TIMEOUT = 60

# ROLES HANDLING


async def assign_verified(ctx):
    verify_msg = await ctx.channel.send(
        f"{random.choice(greeting)}, {ctx.message.author.mention}.\n"
        f"As you might have noticed, I did some banging on the data pipelines, they do not sound clogged. Thus, you're eligible for the role as a `verified` member :robot:\n"
        f"Please react to this message with an optional emoji to gain the `verified` role.\n\n"
        f"`This message will burn in {TIMEOUT} seconds`"
    )
    return verify_msg


async def confirm_verified(ctx):
    confirm_msg = await ctx.channel.send(
        f"Dear {ctx.message.author.mention} :heart:\n"
        f"I assigned you the role as a `verified` member. You're now able to subscribe node(s).\n"
        "See how to subscribe your node(s) here:\n"
        f"> <#993895415873273916>\n"
        "All commands can also be used by DMing the Node Robot:\n"
        "> <#977302927154769971>\n\n"
        f"`This message will burn in {TIMEOUT} seconds`"
    )
    return confirm_msg


async def deny_verified(ctx):
    deny_msg = await ctx.channel.send(
        f"Hi, {ctx.message.author.mention},\n"
        f"Please allow me to DM you. Otherwise, I can't grant you the `verified` member privileges:\n"
        "> * Click the server title at the top of the left menu\n"
        "> * Go to `Privacy Settings`\n"
        "> * Enable/allow `Direct Messages`\n"
        "> * Come back here and write me an message\n"
        "If you're having trouble please contact <@794353079825727500>.\n\n"
        "`This message will burn in 60 seconds`"
    )
    return deny_msg


# REQUEST HANDLING


async def request(ctx):
    msg = await ctx.message.author.send(
            "**`REPORT REQUEST: ADDED TO QUEUE`**\n"
            "**`▒▒▒▒░░░░░░░░░░░░░░░░░░░░░░░░░░`**\n"
            )
    return msg


async def subscriber_role_deny_request(process_msg):
    return await process_msg.edit(
        "**`➭ 1. Add report request to queue`**\n"
        "**`  X  You're not a subscriber`**\n"
        "`  2. Process data`\n"
        "`  3. Report`"
    )


# ERROR
async def send_traceback(bot, trace_message):
    user = await bot.fetch_user("794353079825727500")
    await user.send(f"**ERROR OCCURRED**\n```{trace_message}```")
