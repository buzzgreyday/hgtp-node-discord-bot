import asyncio
import datetime
import logging
from typing import List, Dict

import sqlalchemy.exc
from sqlalchemy.ext.asyncio import async_sessionmaker
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from assets.src.database.crud import CRUD, engine
from assets.src.schemas import OrdinalSchema, PriceSchema, RewardStatsSchema, MetricStatsSchema

app = FastAPI(
    title="The Nodebot",
    description="Created by Buzz Greyday aka hgtp_Michael",
    docs_url="/nodebot/swagger",
)

templates = Jinja2Templates(directory="templates")

app.mount("/static", StaticFiles(directory="static"), name="static")
session = async_sessionmaker(bind=engine, expire_on_commit=False)

db = CRUD()


async def optimize(configuration, node_data_migration_complete=False, ordinal_data_migration_complete=False):
    while True:
        if int(datetime.datetime.now().strftime("%d")) == configuration["general"]["migrate node data (day of month)"] and not node_data_migration_complete:
            print(f"Day is right for node data optimization")
            logging.getLogger("db_optimization").info(
                f"Module: assets/src/database/database.py\n"
                f"Message: The day is {configuration["general"]["migrate node data (day of month)"]} - initiating node data optimization")
            await migrate_old_data(configuration)
            node_data_migration_complete = True
        if int(datetime.datetime.now().strftime("%d")) == configuration["general"]["migrate ordinal data (day of month)"] and not ordinal_data_migration_complete:
            print(f"Day is right for node data optimization")
            logging.getLogger("db_optimization").info(
                f"Module: assets/src/database/database.py\n"
                f"Message: The day is {configuration["general"]["migrate ordinal data (day of month)"]} - initiating ordinal data optimization")
            await migrate_old_ordinals(configuration)
            ordinal_data_migration_complete = True
        if int(datetime.datetime.now().strftime("%d")) != configuration["general"]["migrate node data (day of month)"]:
            print(f"Day is not right for node data optimization")
            node_data_migration_complete = False
        if int(datetime.datetime.now().strftime("%d")) != configuration["general"]["migrate ordinal data (day of month)"]:
            print(f"Day is not right for ordinal data optimization")
            ordinal_data_migration_complete = False
        await asyncio.sleep(3600)

@app.post("/user/create")
async def post_user(data):
    """Creates a new user subscription"""
    return await db.post_user(data, session)


@app.post("/data/create")
async def post_data(data):
    """Inserts node data from automatic check into database file"""
    return await db.post_data(data, session)


@app.post("/data/migrate_data")
async def migrate_old_data(configuration):
    return await db.migrate_old_data(session, configuration)


@app.post("/data/migrate_ordinals")
async def migrate_old_ordinals(configuration):
    return await db.migrate_old_ordinals(session, configuration)


@app.get("/stats/{dag_address}", response_class=HTMLResponse)
async def get_html_page_stats(dag_address, request: Request):
    """Return a html pages showing node stats"""
    return await db.get_html_page_stats(request, templates, dag_address, session)


@app.get("/", response_class=HTMLResponse)
async def get_html_page_index(request: Request):
    """Return Nodebot html index"""
    return await db.get_html_page_index(request, templates)


@app.get("/pages/statistics", response_class=HTMLResponse)
async def get_html_page_statistics(request: Request):
    """Return Nodebot html index"""
    return await db.get_html_page_statistics(request, templates, session)


@app.get("/pages/about", response_class=HTMLResponse)
async def get_html_page_about(request: Request):
    """Return Nodebot html index"""
    return await db.get_html_page_about(request, templates)

@app.get("/user/{name}")
async def get_user(name: str):
    """Returns a list of all user data"""
    return await db.get_user(name, session)


@app.get("/user/ids/layer/{layer}")
async def get_user_ids(layer: int) -> List:
    """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT!!!! Returns a list of all user IDs currently subscribed"""
    return await db.get_user_ids(layer, session)


@app.get("/user/ids/{id_}/{ip}/{port}")
async def get_nodes(id_: str, ip: str, port: int):
    """Return user by ID"""
    return await db.get_nodes(id_, ip, port, session)


@app.get("/user/node/{ip}/{public_port}")
async def get_node(ip: str, public_port: int):
    """Return user by IP and port"""
    return await db.get_node(ip, public_port, session)


@app.get("/user/ids/contact/{contact}/layer/{layer}")
async def get_contact_node_id(contact, layer):
    """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT!!!! Return user by contact"""
    try:
        return await db.get_contact_node_id(contact, layer, session)
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.getLogger("app").error(f"database.py - Database Operation Failed:\n\tDetails: {e}")


@app.get("/data/node/{ip}/{public_port}")
async def get_node_data(ip, public_port):
    """Return latest node data fetched via automatic check by IP and port"""
    return await db.get_node_data(ip, public_port, session)


@app.get("/user/delete")
async def delete_user_entry(data):
    """Delete the user subscription based on name, ip, port"""
    return await db.delete_user_entry(data, session)


@app.delete("/ordinal/{ordinal}/delete")
async def delete_db_ordinal(ordinal: int):
    """Delete the ordinal from the database"""
    return await db.delete_db_ordinal(ordinal, session)


@app.post("/ordinal/create")
async def post_ordinal(data: OrdinalSchema):
    """Inserts node data from automatic check into database"""
    return await db.post_ordinal(data, session)


@app.put("/reward-stat/create")
async def post_reward_stats(data: RewardStatsSchema):
    """insert statistical data into database"""
    return await db.post_reward_stats(data, session)


@app.put("/reward-stat/update")
async def update_reward_stats(data: RewardStatsSchema):
    """insert statistical data into database"""
    return await db.update_reward_stats(data, session)


@app.put("/user/update")
async def update_user(cached_subscriber: Dict):
    """insert statistical data into database"""
    return await db.update_user(cached_subscriber, session)


@app.put("/metric-stat/create")
async def post_metric_stats(data: MetricStatsSchema):
    """insert statistical data into database"""
    return await db.post_metric_stats(data, session)


@app.put("/metric-stat/update")
async def update_metric_stats(data: MetricStatsSchema):
    """insert statistical data into database"""
    return await db.update_metric_stats(data, session)


@app.get("/ordinal/latest")
async def get_latest_db_ordinal():
    """Returns latest ordinal entry from the database"""
    return await db.get_latest_db_ordinal(session)


@app.get("/ordinal/from/{timestamp}")
async def get_ordinals_data_from_timestamp(timestamp: int):
    """Returns ordinal data from timestamp minus"""
    return await db.get_ordinals_data_from_timestamp(int(timestamp), session)


@app.get("/data/from/{timestamp}")
async def get_historic_node_data_from_timestamp(timestamp: int):
    """Returns historic node data from timestamp"""
    return await db.get_historic_node_data_from_timestamp(int(timestamp), session)


@app.get("/price/latest")
async def get_latest_db_price():
    """Returns latest ordinal entry from the database"""
    return await db.get_latest_db_price(session)


@app.post("/price/create")
async def post_prices(data: PriceSchema):
    """Inserts node data from automatic check into database file"""
    return await db.post_prices(data, session)


@app.get("/price/{ordinal_timestamp}")
async def get_timestamp_db_price(ordinal_timestamp: int):
    """Returns price data that best matches the timestamp"""
    return await db.get_timestamp_db_price(ordinal_timestamp, session)
