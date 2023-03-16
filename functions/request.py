import asyncio
import logging
from datetime import datetime
import aiohttp


class Request:
    def __init__(self, url):
        self.url = url

    async def json(self, configuration):
        timeout = aiohttp.ClientTimeout(total=configuration["request"]["timeout"])
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, timeout=timeout) as resp:
                print(resp)
                if resp.status == 200:
                    data = await resp.json()
                    logging.debug(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST SUCCEEDED")
                    return data
                if resp.status == 503:
                    return 503
                else:
                    logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED")
            del resp
            await session.close()


async def node_cluster_data(subscriber, port, configuration):
    cluster_data = []

    if port is not None:
        try:
            print(f"http://{subscriber['ip']}:{port}/{configuration['request']['url']['endings']['node']}")
            node_data = await Request(f"http://{subscriber['ip']}:{port}/{configuration['request']['url']['endings']['node']}").json(configuration)
        except (TimeoutError, aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ServerDisconnectedError) as e:
            node_data = {"state": "Offline", "session": None, "clusterSession": None, "version": None, "host": subscriber["ip"], "publicPort": port, "p2pPort": None, "id": None}
            logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE OFFLINE")

        for k, v in node_data.items():
            if (k == "state") and (v != "Offline"):
                try:
                    cluster_data = await Request(f"http://{subscriber['ip']}:{port}/{configuration['ending']['cluster']}").json(configuration)
                    # cluster_data is a list of dictionaries
                except Exception:
                    pass
            # Else cluster_data from history

        # Marry data

        # After this, check historic data
        return node_data, cluster_data


async def validator_data(configuration):
    validator_mainnet_data = None
    validator_testnet_data = None
    run_again = True
    retry_count = 0
    while run_again:
        try:
            for url in configuration['request']['url']['validator info'][f'testnet']['url']:
                while retry_count < 5:
                    validator_testnet_data = await Request(url).json(configuration)
                    if validator_testnet_data == 502:
                        continue
                    elif validator_testnet_data is not None:
                        run_again = False
                        break
                    else:
                        await asyncio.sleep(3)
                        print(retry_count)
                        retry_count += 1
        except (TimeoutError, aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ServerDisconnectedError) as e:
            await asyncio.sleep(3)
    run_again = True
    retry_count = 0
    while run_again:
        try:
            for url in configuration['request']['url']['validator info'][f'mainnet']['url']:
                while retry_count < 5:
                    validator_mainnet_data = await Request(url).json(configuration)
                    if validator_mainnet_data == 502:
                        continue
                    elif validator_mainnet_data is not None:
                        run_again = False
                        break
                    else:
                        await asyncio.sleep(3)
                        print(retry_count)
                        retry_count += 1
        except (TimeoutError, aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ServerDisconnectedError) as e:
            await asyncio.sleep(3)
    return validator_mainnet_data, validator_testnet_data


