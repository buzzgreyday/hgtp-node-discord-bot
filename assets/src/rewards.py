import asyncio
import logging
import traceback
from datetime import datetime

import aiohttp.client_exceptions
from aiohttp import ClientSession, TCPConnector

from assets.src import preliminaries
from assets.src.schemas import OrdinalSchema, PriceSchema
from assets.src.database.database import post_ordinal, post_prices, delete_db_ordinal
from assets.src.api import Request


class RequestSnapshot:
    def __init__(self, session):
        self.session = session

    async def explorer(self, request_url):
        while True:
            async with self.session.get(request_url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        return data.get("data")
                    else:
                        return
                else:
                    logging.getLogger("rewards").warning(f"rewards.py - Failed getting snapshot data from {request_url}, retrying in 3 seconds")
                    await asyncio.sleep(3)

    async def database(self, request_url):
        while True:
            async with self.session.get(request_url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        return data
                    else:
                        return
                else:
                    logging.getLogger("rewards").warning(
                        f"rewards.py - Failed getting snapshot data from {request_url}, retrying in 3 seconds")
                    await asyncio.sleep(3)


def normalize_timestamp(timestamp):
    return int(round(timestamp))


async def request_prices(session, first_timestamp):
    now = datetime.timestamp(datetime.utcnow())
    while True:
        async with session.get(f"https://api.coingecko.com/api/v3/coins/constellation-labs/market_chart/range?vs_currency=usd&from={first_timestamp}&to={now}&precision=4") as response:
            if response.status == 200:
                data = await response.json()
                # The data, incl. timestamp, is "prices" - timestamp @ idx 0, prices @ idx 1
                for t, p in data.get("prices"):
                    t = int(round(t)/1000)
                    data = PriceSchema(timestamp=t, usd=p)
                    logging.getLogger("rewards").debug(f"rewards.py - Writing price to DB: {data}")
                    await post_prices(data)
                break
            else:
                logging.getLogger("rewards").warning(f"rewards.py - Failed getting price data from Coingecko, retrying in 3 seconds")
                await asyncio.sleep(3)


async def process_ordinal_data(session, url, ordinal, ordinal_data, configuration):
    if isinstance(ordinal_data["timestamp"], str):
        try:
            timestamp = datetime.strptime(ordinal_data["timestamp"], '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            timestamp = datetime.strptime(ordinal_data["timestamp"], '%Y-%m-%dT%H:%M:%SZ')
        ordinal_data["timestamp"] = normalize_timestamp(datetime.timestamp(timestamp))
    while True:
        try:
            db_price_data, status_code = await Request(session, f"http://127.0.0.1:8000/price/{ordinal_data['timestamp']}").db_json(configuration)
        except (asyncio.exceptions.TimeoutError,
                aiohttp.client_exceptions.ClientConnectorError,
                aiohttp.client_exceptions.ClientOSError,
                aiohttp.client_exceptions.ServerDisconnectedError,
                aiohttp.client_exceptions.ClientPayloadError,
        ):
            logging.getLogger("rewards").warning(
                f"rewards.py - Failed getting price data ({ordinal_data['timestamp']}), retrying in 3 seconds")

            await asyncio.sleep(3)
        else:
            if db_price_data and status_code == 200:
                logging.getLogger("rewards").info(
                    f"rewards.py - latest price data is {db_price_data}")
                ordinal_rewards_data = await RequestSnapshot(session).explorer(f"{url}/global-snapshots/{ordinal}/rewards")
                if ordinal_rewards_data:
                    for r_data in ordinal_rewards_data:
                        ordinal_data.update(r_data)
                        data = OrdinalSchema(**ordinal_data)
                        data.usd = db_price_data[1]
                        await post_ordinal(data)
                break
            elif db_price_data is None and status_code == 200:
                logging.getLogger("rewards").warning(
                    f"rewards.py - price data is None db response status code is {status_code}")
                await request_prices(session, ordinal_data['timestamp'])
            else:
                logging.getLogger("rewards").warning(
                f"rewards.py - price data db response status code is {status_code}")

async def fetch_and_process_ordinal_data(session, url, ordinal, configuration):
    logging.getLogger("rewards").info(f"rewards.py - Processing ordinal {ordinal}")
    while True:
        ordinal_data = await RequestSnapshot(session).explorer(f"{url}/global-snapshots/{ordinal}")
        if ordinal_data:
            await process_ordinal_data(session, url, ordinal, ordinal_data, configuration)
            break
        else:
            await asyncio.sleep(3)


async def run(configuration):
    async def process():
        url = "https://be-mainnet.constellationnetwork.io"
        async with ClientSession(connector=TCPConnector(
                # You need to obtain a real (non-self-signed certificate) to run in production
                # ssl=db.ssl_context.load_cert_chain(certfile=ssl_cert_file, keyfile=ssl_key_file)
                # Not intended for production:
                ssl=False)) as session:
            while True:
                now = normalize_timestamp(datetime.utcnow().timestamp())
                latest_snapshot = await RequestSnapshot(session).explorer(f"{url}/global-snapshots/latest")
                if latest_snapshot:
                    latest_ordinal = latest_snapshot.get("ordinal")
                    db_data = await RequestSnapshot(session).database("http://127.0.0.1:8000/ordinal/latest")
                    db_price_data = await RequestSnapshot(session).database("http://127.0.0.1:8000/price/latest")
                    if db_data:
                        first_timestamp = db_price_data[0]
                        first_ordinal = db_data[1]
                        await delete_db_ordinal(first_ordinal)
                    else:
                        first_ordinal = 0
                        # 2021-01-01T00:00:00 = 1609459200
                        first_timestamp = 1609459200

                    if first_timestamp < now:
                        await request_prices(
                            session, first_timestamp
                        )
                    for ordinal in range(first_ordinal, latest_ordinal):
                        await fetch_and_process_ordinal_data(session, url, ordinal, configuration)
                    break
                else:
                    await asyncio.sleep(3)

            await session.close()

    await asyncio.sleep(10)
    times = preliminaries.generate_rewards_runtimes()
    logging.getLogger("rewards").info(f"rewards.py - Runtimes: {times}")
    while True:
        async with asyncio.Semaphore(100):
            if datetime.time(datetime.utcnow()).strftime("%H:%M:%S") in times:
                try:
                    logging.getLogger("rewards").info(f"rewards.py - Starting process")
                    await process()
                except Exception:
                    logging.getLogger("rewards").critical(
                        f"rewards.py - Run process failed:\n"
                        f"\t{traceback.format_exc()}")
            await asyncio.sleep(1)