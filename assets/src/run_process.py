import logging

from assets.src import dt, preliminaries, user, determine_module, api, history
from assets.src.discord import discord
from assets.src.discord.services import bot
from main import version_manager


async def main(ctx, process_msg, requester, cluster_name, layer, _configuration) -> None:
    if requester is None:
        logging.getLogger(__name__).info(f"main.py - Automatic {cluster_name, layer} check initiated")
    else:
        logging.getLogger(__name__).info(f"main.py - Request from {requester} initiated")
    # GET GITHUB VERSION HERE
    dt_start, timer_start = dt.timing()
    process_msg = await discord.update_request_process_msg(process_msg, 1, None)
    cluster_data = await preliminaries.supported_clusters(cluster_name, layer, _configuration)
    ids = await api.get_user_ids(layer, requester, _configuration)

    await bot.wait_until_ready()
    data = await user.process_node_data_per_user(cluster_name, ids, requester, cluster_data, process_msg, version_manager, _configuration)
    process_msg = await discord.update_request_process_msg(process_msg, 5, None)
    data = await determine_module.notify(data, _configuration)
    process_msg = await discord.update_request_process_msg(process_msg, 6, None)
    if not process_msg:
        await history.write(data)
    await discord.send(ctx, process_msg, bot, data, _configuration)
    await discord.update_request_process_msg(process_msg, 7, None)
    dt_stop, timer_stop = dt.timing()
    if requester is None:
        logging.getLogger(__name__).info(
            f"main.py - Automatic {cluster_name, layer} check completed in {round(timer_stop - timer_start, 2)} seconds")
    else:
        logging.getLogger(__name__).info(
            f"main.py - Request from {requester} completed in {round(timer_stop - timer_start, 2)} seconds")