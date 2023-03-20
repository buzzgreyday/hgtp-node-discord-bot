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


async def node_cluster_data(subscriber: dict, port: int, node_data: dict, configuration: dict) -> tuple[dict, dict]:
    async def api_request_type(request_url: str) -> str:
        if ("node" and "info") in request_url.split("/"):
            return "info"
        elif ("cluster" and "info") in request_url.split("/"):
            return "cluster"
        elif ("node" and "metrics") in request_url.split("/"):
            return "metrics"

    async def request_node_data(request_url: str) -> dict:
        data = {}
        retry_count = 0
        run_again = True
        while run_again:
            try:
                data = await Request(request_url).json(configuration)
                if retry_count >= configuration['request']['max retry count']:
                    if await api_request_type(request_url) == "info":
                        data = {"state": "Offline"}
                    break
                elif data == 503:
                    break
                elif data is not None:
                    break
                else:
                    retry_count += 1
                    await asyncio.sleep(configuration['request']['retry sleep'])
            except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientOSError, aiohttp.client_exceptions.ServerDisconnectedError) as e:
                if retry_count >= configuration['request']['max retry count']:
                    if await api_request_type(request_url) == "info":
                        data = {"state": "Offline"}
                    break
                retry_count += 1
                await asyncio.sleep(configuration['request']['retry sleep'])
                logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE @ {subscriber['ip']}:{port} UNREACHABLE - TRIED {retry_count}/{configuration['request']['max retry count']}")
            except aiohttp.client_exceptions.InvalidURL:
                if await api_request_type(request_url):
                    data = {"state": "Offline"}
                break
            except aiohttp.client_exceptions.ClientConnectorError:
                break
        return data
    cluster_data = []
    if port is not None:
        response = await request_node_data(f"http://{subscriber['ip']}:{port}/{configuration['request']['url']['clusters']['url endings']['node info']}")
        node_data.update(response)
        for k, v in node_data.items():
            if (k == "state") and (v != "Offline"):
                cluster_data = await request_node_data(f"http://{str(subscriber['ip'])}:{str(port)}/{str(configuration['request']['url']['clusters']['url endings']['cluster info'])}")

    return node_data, cluster_data


async def supported_clusters(cluster_layer, cluster_names, configuration):
    all_clusters_data = []
    for cluster_name, cluster_info in cluster_names.items():
        for lb_url in cluster_info["url"]:
            response = list(await Request(f"{lb_url}/{configuration['request']['url']['clusters']['url endings']['cluster info']}").json(configuration))
            if response is not None:
                state = "online"
            else:
                state = "offline"
            data = {
                "layer": cluster_layer,
                "cluster name": cluster_name,
                "data": response,
                "state": state
            }
            print(data["layer"], data["cluster name"], data["state"])
        all_clusters_data.append(data)
        del lb_url
    return all_clusters_data


async def validator_data(configuration: dict) -> tuple[list[dict], list[dict]]:
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
                    aiohttp.client_exceptions.ClientOSError, aiohttp.client_exceptions.ServerDisconnectedError) as e:
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
            data = await Request(f"{configuration['request']['url']['github']['api repo url']}/{configuration['request']['url']['github']['url endings']['tessellation']['latest release']}").json(configuration)
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

