import asyncio
import logging
from datetime import datetime
import aiohttp
import aiohttp.client_exceptions
from modules.clusters import all

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


async def request_cluster_data(lb_url, cluster_layer, cluster_name, configuration):
    cluster_resp = await cluster_data(
        f"{lb_url}/{configuration['request']['url']['clusters']['url endings']['cluster info']}", configuration)
    node_resp = await cluster_data(
        f"{lb_url}/{configuration['request']['url']['clusters']['url endings']['node info']}", configuration)
    latest_ordinal, latest_timestamp, addresses = await locate_rewarded_addresses(cluster_layer, cluster_name,
                                                                                          configuration)

    if node_resp is None:
        cluster_state = "offline" ; cluster_id = await all.locate_id_offline(cluster_layer, cluster_name, configuration) ; cluster_session = None
    else:
        cluster_state = str(node_resp['state']).lower() ; cluster_id = node_resp["id"] ; cluster_session = node_resp["clusterSession"]

    cluster = {
        "layer": cluster_layer,
        "cluster name": cluster_name,
        "state": cluster_state,
        "id": cluster_id,
        "pair count": len(cluster_resp),
        "cluster session": cluster_session,
        "latest ordinal": latest_ordinal,
        "latest ordinal timestamp": latest_timestamp,
        "recently rewarded": addresses,
        "pair data": cluster_resp
    }
    await all.update_config_with_latest_values(cluster, configuration)
    del node_resp
    return cluster

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
    return list(filter(lambda x: x in request_url.split("/"), ["node", "cluster", "metrics"]))[0]
async def node_cluster_data(node_data: dict, configuration: dict) -> tuple[dict, dict]:

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
                    f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE @ {node_data['host']}:{node_data['publicPort']} UNREACHABLE - TRIED {retry_count}/{configuration['request']['max retry count']}")
            except aiohttp.client_exceptions.InvalidURL:
                if await api_request_type(request_url):
                    data = {"state": "offline"}
                break
            except aiohttp.client_exceptions.ClientConnectorError:
                break
        return data

    cluster_data = []
    if node_data['publicPort'] is not None:
        response = await safe_request(
            f"http://{node_data['host']}:{node_data['publicPort']}/{configuration['request']['url']['clusters']['url endings']['node info']}")
        node_data.update(response)
        node_data["state"] = node_data["state"]
        for k, v in node_data.items():
            if (k == "state") and (v != "offline"):
                cluster_data = await safe_request(
                    f"http://{str(node_data['host'])}:{str(node_data['publicPort'])}/{str(configuration['request']['url']['clusters']['url endings']['cluster info'])}")
                node_data["nodePairCount"] = len(cluster_data)
                metrics_data = await safe_request(
                    f"http://{str(node_data['host'])}:{str(node_data['publicPort'])}/{str(configuration['request']['url']['clusters']['url endings']['metrics info'])}")
                node_data.update(metrics_data)
        node_data = await request_wallet_data(node_data, configuration)
    return node_data

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

async def reward_check(node_data: dict, all_supported_clusters_data: list):
    # SAME PROCEDURE AS IN CLUSTERS_DATA.MERGE_NODE_DATA
    for lst in all_supported_clusters_data:
        for cluster in lst:
            if (cluster["layer"] == f"layer {node_data['layer']}") and (cluster["cluster name"] == node_data["clusterNames"]):
                if str(node_data["nodeWalletAddress"]) in cluster["recently rewarded"]:
                    node_data["rewardState"] = True
                elif (cluster["recently rewarded"] is None) and (str(node_data["nodeWalletAddress"]) not in cluster["recently rewarded"]):
                        node_data["rewardState"] = False

    return node_data

async def request_wallet_data(node_data, configuration):
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
    async def make_update(data):
        return {
            "nodeWalletBalance": data["data"]["balance"]
        }

    for be_layer, be_names in configuration["request"]["url"]["block explorer"].items():
        if (node_data['clusterNames'] or node_data['formerClusterNames']) in list(be_names.keys()):
            for be_name, be_url in be_names.items():
                if be_name.lower() == (node_data['clusterNames'] or node_data['formerClusterNames']):
                    wallet_data = await safe_request(f"{be_url}/addresses/{node_data['nodeWalletAddress']}/balance")
                    if wallet_data is not None:
                        node_data.update(await make_update(wallet_data))

        else:
            wallet_data = await safe_request(f"{configuration['request']['url']['block explorer']['layer 0']['mainnet']}/addresses/{node_data['nodeWalletAddress']}/balance")
            if wallet_data is not None:
                node_data.update(await make_update(wallet_data))

    return node_data