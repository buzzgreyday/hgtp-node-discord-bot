import asyncio
import logging
from datetime import datetime
import aiohttp
import aiohttp.client_exceptions


class Request:
    def __init__(self, url):
        self.url = url

    async def json(self, configuration):
        timeout = aiohttp.ClientTimeout(total=configuration["request"]["timeout"])
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.debug(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST SUCCEEDED")
                    return data
                if resp.status == 503:
                    logging.debug(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED: SERVICE UNAVAILABLE")
                    return 503
                else:
                    logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED")
            del resp
            await session.close()


async def node_cluster_data(subscriber, port, configuration):
    node_data = []
    cluster_data = []
    if port is not None:
        retry_count = 0
        run_again = True
        while run_again:
            try:
                node_data = await Request(f"http://{subscriber['ip']}:{port}/{configuration['request']['url']['endings']['node']}").json(configuration)
                if retry_count >= configuration['request']['max retry count']:
                    node_data = {"state": "Offline", "session": None, "clusterSession": None, "version": None, "host": subscriber["ip"], "publicPort": port, "p2pPort": None, "id": None}
                    run_again = False
                    break
                elif node_data == 503:
                    run_again = False
                    break
                elif node_data is not None:
                    run_again = False
                    break
                else:
                    retry_count += 1
                    await asyncio.sleep(configuration['request']['retry sleep'])
            except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientOSError, aiohttp.client_exceptions.ServerDisconnectedError) as e:
                if retry_count >= configuration['request']['max retry count']:
                    node_data = {"state": "Offline", "session": None, "clusterSession": None, "version": None, "host": subscriber["ip"], "publicPort": port, "p2pPort": None, "id": None}
                    run_again = False
                    break
                retry_count += 1
                await asyncio.sleep(configuration['request']['retry sleep'])
                logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE @ {subscriber['ip']}:{port} UNREACHABLE - TRIED {retry_count}/{configuration['request']['max retry count']}")
            except aiohttp.client_exceptions.InvalidURL:
                node_data = {"state": "Offline", "session": None, "clusterSession": None, "version": None, "host": subscriber["ip"], "publicPort": port, "p2pPort": None, "id": None}
                run_again = False
                break
            except aiohttp.client_exceptions.ClientConnectorError:
                run_again = False
                break
        retry_count = 0
        run_again = True
        for k, v in node_data.items():
            if (k == "state") and (v != "Offline"):
                while run_again:
                    try:
                        cluster_data = await Request(f"http://{str(subscriber['ip'])}:{str(port)}/{str(configuration['request']['url']['endings']['cluster'])}").json(configuration)
                        if retry_count >= configuration['request']['max retry count']:
                            run_again = False
                            break
                        elif cluster_data == 503:
                            run_again = False
                            break
                        elif cluster_data is not None:
                            run_again = False
                            break
                        else:
                            retry_count += 1
                            await asyncio.sleep(configuration['request']['retry sleep'])

                        # cluster_data is a list of dictionaries
                    except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientOSError, aiohttp.client_exceptions.ServerDisconnectedError) as e:
                        if retry_count >= configuration['request']['max retry count']:
                            run_again = False
                            break
                        retry_count += 1
                        await asyncio.sleep(configuration['request']['retry sleep'])
                        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE CLUSTER @ {subscriber['ip']}:{port} UNREACHABLE")
                    except aiohttp.client_exceptions.InvalidURL:
                        node_data = {"state": "Offline", "session": None, "clusterSession": None, "version": None,
                                     "host": subscriber["ip"], "publicPort": port, "p2pPort": None, "id": None}
                        run_again = False
                        break
                    except aiohttp.client_exceptions.ClientConnectorError:
                        run_again = False
                        break
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
                while retry_count < configuration['request']['max retry count']:
                    validator_testnet_data = await Request(str(url)).json(configuration)
                    if validator_testnet_data == 503:
                        break
                    elif validator_testnet_data is not None:
                        run_again = False
                        break
                    else:
                        await asyncio.sleep(configuration['request']['retry sleep'])
                        retry_count += 1
        except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientConnectorError, aiohttp.client_exceptions.ClientOSError, aiohttp.client_exceptions.ServerDisconnectedError) as e:
            if retry_count >= configuration['request']['max retry count']:
                run_again = False
                break
            await asyncio.sleep(configuration['request']['retry sleep'])
    run_again = True
    retry_count = 0
    while run_again:
        try:
            for url in configuration['request']['url']['validator info'][f'mainnet']['url']:
                while retry_count < configuration['request']['max retry count']:
                    validator_mainnet_data = await Request(str(url)).json(configuration)
                    if validator_mainnet_data == 503:
                        # TRY NEXT KNOWN URL
                        break
                    elif validator_mainnet_data is not None:
                        run_again = False
                        break
                    else:
                        await asyncio.sleep(configuration['request']['retry sleep'])
                        retry_count += 1
        except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientConnectorError, aiohttp.client_exceptions.ClientOSError, aiohttp.client_exceptions.ServerDisconnectedError) as e:
            if retry_count >= configuration['request']['max retry count']:
                run_again = False
                break
            await asyncio.sleep(configuration['request']['retry sleep'])
    return validator_mainnet_data, validator_testnet_data


async def latest_project_version_github(url, configuration):
    data = None
    run_again = True
    retry_count = 0
    while run_again:
        try:
            data = await Request(url).json(configuration)
            if retry_count >= configuration['request']['max retry count']:
                run_again = False
                break
            elif data is not None:
                run_again = False
                break
            else:
                retry_count += 1
                await asyncio.sleep(configuration['request']['retry sleep'])
            # cluster_data is a list of dictionaries
        except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientOSError,
                aiohttp.client_exceptions.ServerDisconnectedError) as e:
            if retry_count >= configuration['request']['max retry count']:
                run_again = False
                break
            retry_count += 1
            await asyncio.sleep(configuration['request']['retry sleep'])
            logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - GITHUB URL {url} UNREACHABLE")
    return data["tag_name"][1:]

