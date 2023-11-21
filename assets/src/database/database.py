from sqlalchemy.ext.asyncio import async_sessionmaker
from fastapi import FastAPI

from assets.src.database.crud import CRUD, engine
from assets.src.schemas import OrdinalSchema, PriceSchema

app = FastAPI(
    title="Hypergraph Node Status Bot", description="This is a Node Bot", docs_url="/"
)
session = async_sessionmaker(bind=engine, expire_on_commit=False)
db = CRUD()


@app.post("/user/create")
async def post_user(data):
    """Creates a new user subscription"""
    return await db.post_user(data, session)


@app.post("/data/create")
async def post_data(data):
    """Inserts node data from automatic check into database file"""
    return await db.post_data(data, session)


@app.get("/user/{name}")
async def get_user(name: str):
    """Returns a list of all user data"""
    return await db.get_user(name, session)


@app.get("/user/ids/layer/{layer}")
async def get_user_ids(layer: int):
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
    return await db.get_contact_node_id(contact, layer, session)


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
    """Inserts node data from automatic check into database file"""
    return await db.post_ordinal(data, session)


@app.get("/ordinal/latest")
async def get_latest_db_ordinal():
    """Returns latest ordinal entry from the database"""
    return await db.get_latest_db_ordinal(session)


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
