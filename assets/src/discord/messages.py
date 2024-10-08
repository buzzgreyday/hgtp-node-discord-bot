import random

import nextcord

greeting = ["Hi", "Hallo", "Greetings", "Hey"]
TIMEOUT = 60


class ProgressBar:

    def __init__(self, bar_size, num_processes, title=None, text=None):

        self.title = title if title else None
        self.text = text if text else None
        self.process = 1
        self.bar_size = bar_size
        self.num_processes = num_processes
        self.completed = "▓" * self.process
        self.remaining = "░" * (self.bar_size - self.process)
        self.bar = f"{self.completed}{self.remaining}"

    def draw_bar(self, process: int):
        process = round(process / self.num_processes * self.bar_size)
        self.completed = "▓" * process
        self.remaining = "░" * (self.bar_size - process)
        self.bar = f"{self.completed}{self.remaining}"
        if self.title and self.text:
            return f"{self.title}", f"{self.text}", f"{self.bar}"
        elif self.title and not self.text:
            return f"{self.title}", f"{self.bar}"
        elif not self.title and self.text:
            return f"{self.text}", f"{self.bar}"
        else:
            return f"{self.bar}"


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
        f"**{ctx.message.author.mention}, please allow DMs from server members.**\n"
        f"This will allow the Nodebot to message you privately:\n"
        "1. Click the server title at the top of the left menu\n"
        "2. Navigate to `Privacy Settings`\n"
        "3. Enable `Direct Messages`\n"
        "If you're having trouble please contact <@794353079825727500>.\n\n"
        "`This message will burn in 60 seconds`"
    )
    return deny_msg


# REQUEST HANDLING
async def send_request_process_msg(bot, ctx):
    try:
        msg = await ctx.message.author.send(
            "### **`REPORT REQUEST: ADDED TO QUEUE`**\n"
        )
        return msg
    except nextcord.Forbidden:
        return None


async def update_request_process_msg(process_msg, process_num):
    if process_msg is None:
        return None
    elif process_msg is not None:
        if process_num == 1:
            return await process_msg.edit(
                "### **`REPORT REQUEST: FETCH USER`**\n"
            )
        elif process_num == 2:
            return await process_msg.edit(
                "### **`REPORT REQUEST: PROCESSING`**\n"
            )
        elif process_num == 3:
            return await process_msg.edit(
                "### **`REPORT REQUEST: PROCESSING`**\n"
            )
        elif process_num == 4:
            return await process_msg.edit(
                "### **`REPORT REQUEST: PROCESSING`**\n"
            )
        elif process_num == 5:
            return await process_msg.edit(
                "### **`REPORT REQUEST: BUILDING`**\n"
            )
        elif process_num == 6:
            return await process_msg.edit(
                "### **`REPORT REQUEST: SENT`**\n"
            )
        elif process_num == 7:
            return await process_msg.edit(
                "### **`REPORT REQUEST: ERROR, PLEASE REPORT!`**\n"
            )


async def subscriber_role_deny_request(process_msg):
    return await process_msg.edit(
        "### **`NO SUBSCRIPTION EXISTS`**\n"
    )


# ERROR
async def send_traceback(bot, trace_message):
    user = await bot.fetch_user(int(794353079825727500))
    await user.send(f"**ERROR OCCURRED**\n```{trace_message}```")


async def command_error(ctx, bot):
    embed = nextcord.Embed(
        title="There's a problem here".upper(), color=nextcord.Color.orange()
    )
    embed.add_field(
        name=f"\U00002328` {ctx.message.content}`",
        value=f"`ⓘ The above command is invalid`",
        inline=False,
    )
    embed.set_author(
        name=ctx.message.author,
        icon_url=bot.get_user(ctx.message.author.id).display_avatar.url,
    )
    return embed

