import logging
from datetime import datetime

import aiohttp

import configuration


class Request:
    def __init__(self, url):
        self.url = url

    async def json(self):
        timeout = aiohttp.ClientTimeout(total=configuration.aiohttp_timeout)
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.debug(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST SUCCEEDED")
                else:
                    logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED")
            del resp
            await session.close()
        return data


async def node_data(subscriber, port):
    cluster_data = []

    if port is not None:
        try:
            node_data = await Request(f"http://{subscriber['ip']}:{port}/{configuration.node}").json()
        except Exception:
            node_data = {"state": "Offline", "session": None, "clusterSession": None, "version": None, "host": subscriber["ip"], "publicPort": port, "p2pPort": None, "id": None}
            logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE OFFLINE")

        for k, v in node_data.items():
            if (k == "state") and (v != "Offline"):
                try:
                    cluster_data = await Request(f"http://{subscriber['ip']}:{port}/{configuration.cluster}").json()
                    # cluster_data is a list of dictionaries
                except Exception:
                    pass
            # Else cluster_data from history

        # Marry data

        # After this, check historic data
        return node_data, cluster_data
