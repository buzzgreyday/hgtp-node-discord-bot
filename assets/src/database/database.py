import os

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from fastapi import FastAPI

from crud import CRUD

load_dotenv()

engine = create_async_engine(
    url=os.getenv("DB_URL"),
    echo=True
)

api = FastAPI(title="HGTP Node Status Bot", description="This is a Node Bot", docs_url="/")
session = async_sessionmaker(
    bind=engine,
    expire_on_commit=False
)
db = CRUD()

@api.post("/user/create")
async def post_user(data):
    """Creates a new user subscription"""
    await db.post_user(data, session)


@api.post("/data/create")
async def post_data(data):
    """Inserts node data from automatic check into database file"""
    await db.post_data(data, session)


@api.get("/user")
async def get_users():
    """Returns a list of all user data"""
    await db.get_users(session)


@api.get("/user/ids/layer/{layer}")
async def get_user_ids(layer: int):
    """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT!!!! Returns a list of all user IDs currently subscribed"""
    await db.get_user_ids(layer, session)


@api.get("/user/ids/{id_}/{ip}/{port}")
async def get_nodes(id_: str, ip: str, port: int):
    """Return user by ID"""
    await db.get_nodes(id_, ip, port, session)


@api.get("/user/node/{ip}/{public_port}")
async def get_node(ip: str, public_port: int):
    """Return user by IP and port"""
    await db.get_node(ip, public_port, session)


@api.get("/user/ids/contact/{contact}/layer/{layer}")
async def get_contact_node_id(contact, layer):
    """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT!!!! Return user by contact"""
    await db.get_contact_node_id(contact, layer, session)


@api.get("/data/node/{ip}/{public_port}")
async def get_node_data(ip, public_port):
    """Return latest node data fetched via automatic check by IP and port"""
    await db.get_node_data(ip, public_port, session)