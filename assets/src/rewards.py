import asyncio
import logging
from datetime import datetime

from aiohttp import ClientSession, TCPConnector

from assets.src import preliminaries
from assets.src.schemas import OrdinalSchema, PriceSchema
from assets.src.database.database import post_ordinal, post_prices, delete_db_ordinal
from assets.src.api import safe_request, Request

def normalize_timestamp(timestamp):
    return int(round(timestamp))


async def request_snapshot(session, request_url):
    while True:
        async with session.get(request_url) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("data")
            else:
                logging.getLogger(__name__).warning(f"rewards.py - Failed getting snapshot data from {request_url}, retrying in 3 seconds")
                await asyncio.sleep(3)


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
                    logging.getLogger(__name__).debug(f"rewards.py - Writing price to DB: {data}")
                    await post_prices(data)
                break
            else:
                logging.getLogger(__name__).warning(f"rewards.py - Failed getting price data from Coingecko, retrying in 3 seconds")
                await asyncio.sleep(3)


async def process_ordinal_data(session, url, ordinal, ordinal_data, configuration):
    if isinstance(ordinal_data["timestamp"], str):
        try:
            timestamp = datetime.strptime(ordinal_data["timestamp"], '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            timestamp = datetime.strptime(ordinal_data["timestamp"], '%Y-%m-%dT%H:%M:%SZ')
        ordinal_data["timestamp"] = normalize_timestamp(datetime.timestamp(timestamp))
    while True:
        db_price_data, status_code = await Request(session, f"http://127.0.0.1:8000/price/{ordinal_data['timestamp']}").db_json(configuration)
        if db_price_data:
            ordinal_rewards_data = await request_snapshot(session, f"{url}/global-snapshots/{ordinal}/rewards")
            if ordinal_rewards_data:
                for r_data in ordinal_rewards_data:
                    ordinal_data.update(r_data)
                    data = OrdinalSchema(**ordinal_data)
                    data.usd = db_price_data[1]
                    await post_ordinal(data)
            break
        else:
            await request_prices(session, ordinal_data['timestamp'])

async def fetch_and_process_ordinal_data(session, url, ordinal, configuration):
    logging.getLogger(__name__).debug(f"rewards.py - Processing ordinal {ordinal}")
    while True:
        ordinal_data = await request_snapshot(session, f"{url}/global-snapshots/{ordinal}")
        if ordinal_data:
            await process_ordinal_data(session, url, ordinal, ordinal_data, configuration)
            break
        else:
            await asyncio.sleep(3)


async def run(configuration):
    await asyncio.sleep(10)
    times = preliminaries.generate_rewards_runtimes()
    logging.getLogger(__name__).info(f"rewards.py - Runtimes: {times}")
    while True:
        async with asyncio.Semaphore(8):
            if datetime.time(datetime.utcnow()).strftime("%H:%M:%S") in times:
                urls = ["https://be-mainnet.constellationnetwork.io"]
                async with ClientSession(connector=TCPConnector(
                        # You need to obtain a real (non-self-signed certificate) to run in production
                        # ssl=db.ssl_context.load_cert_chain(certfile=ssl_cert_file, keyfile=ssl_key_file)
                        # Not intended for production:
                        ssl=False)) as session:
                    for url in urls:
                        while True:
                            now = normalize_timestamp(datetime.utcnow().timestamp())
                            latest_snapshot = await request_snapshot(session, f"{url}/global-snapshots/latest")
                            if latest_snapshot:
                                latest_ordinal = latest_snapshot.get("ordinal")
                                db_data, status_code = await safe_request(session, "http://127.0.0.1:8000/ordinal/latest", configuration)
                                db_price_data, status_code = await safe_request(session, "http://127.0.0.1:8000/price/latest", configuration)
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
            await asyncio.sleep(1)