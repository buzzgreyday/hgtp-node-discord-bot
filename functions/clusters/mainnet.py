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
                elif resp.status == 503:
                    logging.debug(
                        f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED: SERVICE UNAVAILABLE")
                    return 503
                else:
                    logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED")
            del resp
            await session.close()

    async def text(self, strings: list | tuple, configuration: dict):
        async def find_in_text(text, strings: list | tuple):
            results = []

            for line in text.split('\n'):
                if not line.startswith('#'):
                    for i, item in enumerate(strings):
                        idx = text.find(item)
                        line_start = text[idx:].split('\n')
                        value = line_start[0].split(' ')[1]
                        results.append(value)
                        del (value, line_start, idx)
                        if i >= len(strings):
                            break
                        else:
                            pass
                    break
            return results

        timeout = aiohttp.ClientTimeout(total=configuration["request"]["timeout"])
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, timeout=timeout) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    logging.debug(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST SUCCEEDED")
                    strings = await find_in_text(text, strings)
                    del text
                    return strings
                elif resp.status == 503:
                    logging.debug(
                        f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED: SERVICE UNAVAILABLE")
                    return 503
                else:
                    logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED")
            del resp
            await session.close()


async def locate_rewarded_addresses(cluster_layer, cluster_name, configuration):
    try:
        latest_ordinal, latest_timestamp = \
            await snapshot(
                f"{configuration['request']['url']['block explorer'][cluster_layer][cluster_name]}"
                f"/global-snapshots/latest", configuration)
        tasks = []
        for ordinal in range(latest_ordinal-50, latest_ordinal):
            tasks.append(asyncio.create_task(snapshot_rewards(
                f"{configuration['request']['url']['block explorer'][cluster_layer][cluster_name]}"
                f"/global-snapshots/{ordinal}/rewards", configuration
            )))
        addresses = []
        for task in tasks:
            addresses.extend(await task); addresses = list(set(addresses))
    except KeyError:
        latest_ordinal = None; latest_timestamp = None; addresses = []

    # addresses = (address_generator for address_generator in addresses)
    return latest_ordinal, latest_timestamp, addresses


async def api_request_type(request_url: str) -> str:
    if "node" in request_url.split("/"):
        return "info"
    elif "cluster" in request_url.split("/"):
        return "cluster"
    elif "metrics" in request_url.split("/"):
        return "metrics"

async def node_cluster_data(subscriber: dict, port: int, node_data: dict, configuration: dict) -> tuple[dict, dict]:

    async def safe_request(request_url: str) -> dict:
        data = {}
        retry_count = 0
        run_again = True
        while run_again:
            try:
                if await api_request_type(request_url) in ("info", "cluster"):
                    data = await Request(request_url).json(configuration)
                elif await api_request_type(request_url) == "metrics":
                    strings = ('process_uptime_seconds{application=',
                               'system_cpu_count{application=',
                               'system_load_average_1m{application=',
                               'disk_free_bytes{application=',
                               'disk_total_bytes{application=')
                    strings = await Request(request_url).text(strings, configuration)
                    data = {
                        "clusterAssociationTime": strings[0],
                        "cpuCount": strings[1],
                        "1mSystemLoadAverage": strings[2],
                        "diskSpaceFree": strings[3],
                        "diskSpaceTotal": strings[4]
                    }
                if retry_count >= configuration['request']['max retry count']:
                    if await api_request_type(request_url) == "info":
                        data = {"state": "offline"}
                    break
                elif data == 503:
                    break
                elif data is not None:
                    break
                else:
                    retry_count += 1
                    await asyncio.sleep(configuration['request']['retry sleep'])
            except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientOSError,
                    aiohttp.client_exceptions.ServerDisconnectedError) as e:
                if retry_count >= configuration['request']['max retry count']:
                    if await api_request_type(request_url) == "info":
                        data = {"state": "offline"}
                    break
                retry_count += 1
                await asyncio.sleep(configuration['request']['retry sleep'])
                logging.info(
                    f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE @ {subscriber['ip']}:{port} UNREACHABLE - TRIED {retry_count}/{configuration['request']['max retry count']}")
            except aiohttp.client_exceptions.InvalidURL:
                if await api_request_type(request_url):
                    data = {"state": "offline"}
                break
            except aiohttp.client_exceptions.ClientConnectorError:
                break
        return data

    cluster_data = []
    if port is not None:
        response = await safe_request(
            f"http://{subscriber['ip']}:{port}/{configuration['request']['url']['clusters']['url endings']['node info']}")
        node_data.update(response)
        node_data["state"] = node_data["state"].lower()
        for k, v in node_data.items():
            if (k == "state") and (v != "offline"):
                cluster_data = await safe_request(
                    f"http://{str(subscriber['ip'])}:{str(port)}/{str(configuration['request']['url']['clusters']['url endings']['cluster info'])}")
                metrics_data = await safe_request(
                    f"http://{str(subscriber['ip'])}:{str(port)}/{str(configuration['request']['url']['clusters']['url endings']['metrics info'])}")
                node_data.update(metrics_data)
    return node_data, cluster_data

async def cluster_data(request_url: str, configuration: dict):

    async def safe_request(request_url: str):
        data = {}
        retry_count = 0
        run_again = True
        while run_again:
            try:
                data = await Request(request_url).json(configuration)
                if retry_count >= configuration['request']['max retry count']:
                    if await api_request_type(request_url) == "cluster":
                        data = []
                    elif await api_request_type(request_url) == "info":
                        data = None
                    break
                elif data == 503:
                    if await api_request_type(request_url) == "cluster":
                        data = []
                    elif await api_request_type(request_url) == "info":
                        data = None
                    break
                elif data is not None:
                    break
                else:
                    retry_count += 1
                    await asyncio.sleep(configuration['request']['retry sleep'])
            except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientOSError,
                    aiohttp.client_exceptions.ServerDisconnectedError) as e:
                if retry_count >= configuration['request']['max retry count']:
                    if await api_request_type(request_url) == "cluster":
                        data = []
                    elif await api_request_type(request_url) == "info":
                        data = None
                    break
                retry_count += 1
                await asyncio.sleep(configuration['request']['retry sleep'])
                logging.info(
                    f"{datetime.utcnow().strftime('%H:%M:%S')} - CLUSTER @ {request_url} UNREACHABLE - TRIED {retry_count}/{configuration['request']['max retry count']}")
            except aiohttp.client_exceptions.InvalidURL:
                if await api_request_type(request_url) == "cluster":
                    data = []
                elif await api_request_type(request_url) == "info":
                    data = None
                break
            except aiohttp.client_exceptions.ClientConnectorError:
                if await api_request_type(request_url) == "cluster":
                    data = []
                elif await api_request_type(request_url) == "info":
                    data = None
                break
        return data

    if await api_request_type(request_url) == "cluster":
        return list(await safe_request(request_url))
    elif await api_request_type(request_url) == "info":
        return await safe_request(request_url)

async def wallet_data(request_url: str, configuration: dict):
    async def safe_request(request_url: str):
        data = {}
        retry_count = 0
        run_again = True
        while run_again:
            try:
                data = await Request(request_url).json(configuration)
                if retry_count >= configuration['request']['max retry count']:
                    data = None
                    break
                elif data == 503:
                    data = None
                    break
                elif data is not None:
                    break
                else:
                    retry_count += 1
                    await asyncio.sleep(configuration['request']['retry sleep'])
            except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientOSError,
                    aiohttp.client_exceptions.ServerDisconnectedError) as e:
                if retry_count >= configuration['request']['max retry count']:
                    data = None
                    break
                retry_count += 1
                await asyncio.sleep(configuration['request']['retry sleep'])
                logging.info(
                    f"{datetime.utcnow().strftime('%H:%M:%S')} - CLUSTER @ {request_url} UNREACHABLE - TRIED {retry_count}/{configuration['request']['max retry count']}")
            except aiohttp.client_exceptions.InvalidURL:
                data = None
                break
            except aiohttp.client_exceptions.ClientConnectorError:
                data = None
                break
        return data

    return await safe_request(request_url)

async def validator_data(configuration: dict):
    async def safe_request(validator_urls: list) -> list[dict]:
        validator_network_data = None
        run_again = True
        retry_count = 0
        while run_again:
            try:
                for url in validator_urls:
                    while retry_count < configuration['request']['max retry count']:
                        validator_network_data = await Request(str(url)).json(configuration)
                        if validator_network_data == 503:
                            break
                        elif validator_network_data is not None:
                            run_again = False
                            break
                        else:
                            await asyncio.sleep(configuration['request']['retry sleep'])
                            retry_count += 1
            except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientConnectorError,
                    aiohttp.client_exceptions.ClientOSError,
                    aiohttp.client_exceptions.ServerDisconnectedError) as e:
                if retry_count >= configuration['request']['max retry count']:
                    break
                await asyncio.sleep(configuration['request']['retry sleep'])
        return validator_network_data["data"]

    validator_testnet_data = await safe_request(configuration['request']['url']['validator info']['testnet']['url'])
    validator_mainnet_data = await safe_request(configuration['request']['url']['validator info']['mainnet']['url'])

    return validator_mainnet_data, validator_testnet_data

async def latest_project_version_github(configuration):
    data = None
    run_again = True
    retry_count = 0
    while run_again:
        try:
            data = await Request(
                f"{configuration['request']['url']['github']['api repo url']}/{configuration['request']['url']['github']['url endings']['tessellation']['latest release']}").json(
                configuration)
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
                break
            retry_count += 1
            await asyncio.sleep(configuration['request']['retry sleep'])
            logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - GITHUB URL UNREACHABLE")
    return data["tag_name"][1:]

async def snapshot(request_url, configuraton):


    async def safe_request(request_url: str, configuration):
        data = {}
        retry_count = 0
        run_again = True
        while run_again:
            try:
                data = await Request(request_url).json(configuration)
                if retry_count >= configuration['request']['max retry count']:
                    data = None
                    break
                elif data == 503:
                    data = None
                    break
                elif data is not None:
                    break
                else:
                    retry_count += 1
                    await asyncio.sleep(configuration['request']['retry sleep'])
            except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientOSError,
                    aiohttp.client_exceptions.ServerDisconnectedError) as e:
                if retry_count >= configuration['request']['max retry count']:
                    data = None
                    break
                retry_count += 1
                await asyncio.sleep(configuration['request']['retry sleep'])
                logging.info(
                    f"{datetime.utcnow().strftime('%H:%M:%S')} - CLUSTER @ {request_url} UNREACHABLE - TRIED {retry_count}/{configuration['request']['max retry count']}")
            except aiohttp.client_exceptions.InvalidURL:
                data = None
                break
            except aiohttp.client_exceptions.ClientConnectorError:
                data = None
                break
        return data

    data = await safe_request(request_url, configuraton)
    if data is not None:
        ordinal = data["data"]["ordinal"]
        timestamp = datetime.strptime(data["data"]["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
        return ordinal, timestamp
    elif data is None:
        ordinal = None
        timestamp = None
        return ordinal, timestamp

async def snapshot_rewards(request_url, configuration):
    async def safe_request(request_url: str, configuration):
        data = {}
        retry_count = 0
        run_again = True
        while run_again:
            try:
                data = await Request(request_url).json(configuration)
                if retry_count >= configuration['request']['max retry count']:
                    data = None
                    break
                elif data == 503:
                    data = None
                    break
                elif data is not None:
                    break
                else:
                    retry_count += 1
                    await asyncio.sleep(configuration['request']['retry sleep'])
            except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientOSError,
                    aiohttp.client_exceptions.ServerDisconnectedError) as e:
                if retry_count >= configuration['request']['max retry count']:
                    data = None
                    break
                retry_count += 1
                await asyncio.sleep(configuration['request']['retry sleep'])
                logging.info(
                    f"{datetime.utcnow().strftime('%H:%M:%S')} - CLUSTER @ {request_url} UNREACHABLE - TRIED {retry_count}/{configuration['request']['max retry count']}")
            except aiohttp.client_exceptions.InvalidURL:
                data = None
                break
            except aiohttp.client_exceptions.ClientConnectorError:
                data = None
                break
        return data

    data = await safe_request(request_url, configuration)
    addresses = []
    for data_dictionary in data["data"]:
        addresses.append(data_dictionary["destination"])
    return addresses