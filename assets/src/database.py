import datetime
import logging
from typing import Optional
from pathlib import Path
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select
from fastapi import FastAPI, Depends
from fastapi.encoders import jsonable_encoder

from assets.src.schemas import User as UserModel
from assets.src.schemas import Node as NodeModel

WORKING_DIR = f'{str(Path.home())}/bot'
engine = create_async_engine(f"sqlite+aiosqlite:///{WORKING_DIR}/assets/data/db/database.db", connect_args={"check_same_thread": False})

SessionLocal = async_sessionmaker(engine, class_=AsyncSession)

api = FastAPI()


class SQLBase(DeclarativeBase):
    pass


@api.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(SQLBase.metadata.create_all)


class User(SQLBase):
    """SQL Base for user subscription data"""
    __tablename__ = "users"

    index: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    wallet: Mapped[str]
    id: Mapped[str]
    ip: Mapped[str]
    public_port: Mapped[int]
    layer: Mapped[int]
    contact: Mapped[str]
    date: Mapped[datetime.datetime]
    type: Mapped[str]


class NodeData(SQLBase):
    """SQL Base for automatic check node data"""

    __tablename__ = "data"

    index: Mapped[int] = mapped_column(primary_key=True)
    one_m_system_load_average: Mapped[Optional[float]] = mapped_column(nullable=True)
    cluster_association_time: Mapped[Optional[float]] = mapped_column(nullable=True)
    cluster_connectivity: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_dissociation_time: Mapped[Optional[float]] = mapped_column(nullable=True)
    cluster_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    last_known_cluster_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_peer_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    cluster_state: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_version: Mapped[Optional[str]] = mapped_column(nullable=True)
    contact: Mapped[Optional[str]] = mapped_column(nullable=True)
    cpu_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    disk_space_free: Mapped[Optional[float]] = mapped_column(nullable=True)
    disk_space_total: Mapped[Optional[float]] = mapped_column(nullable=True)
    former_cluster_association_time: Mapped[Optional[float]] = mapped_column(nullable=True)
    former_cluster_connectivity: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_node_cluster_session: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_cluster_dissociation_time: Mapped[Optional[float]] = mapped_column(nullable=True)
    former_cluster_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_cluster_peer_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    former_cluster_state: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_reward_state: Mapped[Optional[bool]] = mapped_column(nullable=True)
    former_timestamp_index: Mapped[Optional[datetime.datetime]] = mapped_column(nullable=True)
    ip: Mapped[Optional[str]] = mapped_column(nullable=True)
    id: Mapped[Optional[str]] = mapped_column(nullable=True)
    last_notified_timestamp: Mapped[Optional[datetime.datetime]] = mapped_column(nullable=True)
    latest_cluster_session: Mapped[Optional[str]] = mapped_column(nullable=True)
    latest_version: Mapped[Optional[str]] = mapped_column(nullable=True)
    layer: Mapped[Optional[int]] = mapped_column(nullable=True)
    name: Mapped[Optional[str]] = mapped_column(nullable=True)
    node_cluster_session: Mapped[Optional[str]] = mapped_column(nullable=True)
    node_peer_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    wallet_address: Mapped[Optional[str]] = mapped_column(nullable=True)
    wallet_balance: Mapped[Optional[float]] = mapped_column(nullable=True)
    notify: Mapped[Optional[bool]] = mapped_column(nullable=True)
    p2p_port: Mapped[Optional[int]] = mapped_column(nullable=True)
    public_port: Mapped[Optional[int]] = mapped_column(nullable=True)
    reward_false_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    reward_state: Mapped[Optional[bool]] = mapped_column(nullable=True)
    reward_true_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    state: Mapped[Optional[str]] = mapped_column(nullable=True)
    timestamp_index: Mapped[Optional[datetime.datetime]] = mapped_column(nullable=True)
    version: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_check_ordinal: Mapped[Optional[str]] = mapped_column(nullable=True)


async def get_db() -> AsyncSession:
    async with SessionLocal() as session:
        yield session


async def get_next_index(Model, db: AsyncSession) -> int:
    """Fetch the last assigned index from the separate table"""
    result = await db.execute(select(Model.index).order_by(Model.index.desc()).limit(1))
    last_index = result.scalar_one_or_none()
    return 0 if last_index is None else last_index + 1


@api.post("/user/create")
async def post_user(data: UserModel, db: AsyncSession = Depends(get_db)):
    """Creates a new user subscription"""
    next_index = await get_next_index(User, db)
    data.index = next_index
    data.date = datetime.datetime.utcnow()
    data_dict = data.dict()
    user = User(**data_dict)
    result = await db.execute(select(User).where((User.ip == data.ip) & (User.public_port == data.public_port)))
    # You only need one result that matches
    result = result.fetchone()
    if result:
        logging.getLogger(__name__).info(f"main.py - The user {data.name} already exists for {data.ip}:{data.public_port}")
    else:
        db.add(user)
        await db.commit()
        await db.refresh(user)
        logging.getLogger(__name__).info(f"main.py - A new subscription recorded for {data.name} ({data.ip}:{data.public_port})")
    return jsonable_encoder(data_dict)


@api.post("/data/create")
async def post_data(data: NodeModel, db: AsyncSession = Depends(get_db)):
    """Inserts node data from automatic check into database file"""
    next_index = await get_next_index(NodeData, db)
    data.index = next_index
    data_dict = data.dict()
    node_data = NodeData(**data_dict)
    db.add(node_data)
    await db.commit()
    await db.refresh(node_data)
    return jsonable_encoder(data_dict)



@api.get("/user")
async def get_users(db: AsyncSession = Depends(get_db)):
    """Returns a list of all user data"""
    results = await db.execute(select(User))
    users = results.scalars().all()
    return {"users": users}


@api.get("/user/ids")
async def get_user_ids(db: AsyncSession = Depends(get_db)):
    """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT!!!! Returns a list of all user IDs currently subscribed"""
    list_of_tuples = []
    results = await db.execute(select(User))
    ids = results.scalars().all()
    for values in ids:
        list_of_tuples.append((values.id, values.ip, values.public_port, values.layer))
    # return list(set(ids))
    return list_of_tuples


@api.get("/user/ids/{id_}/{ip}/{port}")
async def get_nodes(id_: str, ip: str, port: int, db: AsyncSession = Depends(get_db)):
    """Return user by ID"""
    results = await db.execute(select(User).where((User.id == id_) & (User.ip == ip) & (User.public_port == port)))
    nodes = results.scalars().all()
    return nodes


@api.get("/user/node/{ip}/{public_port}")
async def get_node(ip: str, public_port: int, db: AsyncSession = Depends(get_db)):
    """Return user by IP and port"""
    results = await db.execute(select(User).where((User.ip == ip) & (User.public_port == public_port)))
    node = results.scalars().all()
    return {"node": node}


@api.get("/user/ids/contact/{contact}")
async def get_contact_node_id(contact, db: AsyncSession = Depends(get_db)):
    """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT!!!! Return user by contact"""
    list_of_tuples = []
    results = await db.execute(select(User).where(User.contact == contact))
    ids = results.scalars().all()
    for values in ids:
        list_of_tuples.append((values.id, values.ip, values.public_port, values.layer))
    # return list(set(ids))
    return list_of_tuples


@api.get("/data/node/{ip}/{public_port}")
async def get_node_data(ip, public_port, db: AsyncSession = Depends(get_db)):
    """Return latest node data fetched via automatic check by IP and port"""
    results = await db.execute(
                    select(NodeData)
                    .where((NodeData.ip == ip) & (NodeData.public_port == public_port))
                    .order_by(NodeData.timestamp_index.desc())
                    .limit(1))
    node = results.scalar_one_or_none()
    return node
