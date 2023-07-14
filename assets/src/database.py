import datetime
from typing import List

import sqlalchemy
import uuid as uuid

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select
from fastapi import FastAPI, Depends
from fastapi.encoders import jsonable_encoder

from assets.src.schemas import User as UserModel

engine = create_async_engine("sqlite+aiosqlite:///assets/data/db/db.sqlite3", connect_args={"check_same_thread": False})

SessionLocal = async_sessionmaker(engine, class_=AsyncSession)

api = FastAPI()


class SQLBase(DeclarativeBase):
    pass


@api.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(SQLBase.metadata.create_all)


class User(SQLBase):
    __tablename__ = "users"

    uuid: Mapped[str] = mapped_column(default=lambda: str(uuid.uuid4()), index=True, nullable=False, primary_key=True)
    name: Mapped[str]
    id: Mapped[str]
    ip: Mapped[str]
    public_port: Mapped[int]
    layer: Mapped[int]
    contact: Mapped[str]
    date: Mapped[datetime.datetime]
    type: Mapped[str]

class NodeData(SQLBase):
    __tablename__ = "data"

    one_m_system_load_average: Mapped[float]
    cluster_association_time: Mapped[float]
    cluster_connectivity: Mapped[str]
    cluster_dissociation_time: Mapped[float]
    cluster_name: Mapped[str]
    cluster_peer_count: Mapped[int]
    cluster_state: Mapped[str]
    cluster_version: Mapped[str]
    contact: Mapped[str]
    cpu_count: Mapped[int]
    disk_space_free: Mapped[float]
    disk_space_total: Mapped[float]
    former_cluster_association_time: Mapped[float]
    former_cluster_connectivity: Mapped[str]
    former_cluster_dissociation_time: Mapped[float]
    former_cluster_name: Mapped[str]
    former_cluster_peer_count: Mapped[int]
    former_reward_state: Mapped[bool]
    former_timestamp_index: Mapped[datetime.datetime]
    ip: Mapped[str]
    id: Mapped[str] = mapped_column(sqlalchemy.String, index=True, nullable=False, primary_key=True)
    """last_notified_timestamp
    latest_cluster_session
    latest_version
    layer
    name
    node_cluster_session
    node_peer_count
    wallet_address
    wallet_balance
    notify
    p2p_port
    public_port
    reward_false_count
    reward_state
    reward_true_count
    state
    timestamp_index
    version"""


async def get_db() -> AsyncSession:
    async with SessionLocal() as session:
        yield session


@api.post("/user/create")
async def post_user(data: UserModel, db: AsyncSession = Depends(get_db)):
    data_dict = data.dict()
    user = User(**data_dict)
    result = await db.execute(select(User).where((User.ip == data.ip) & (User.public_port == data.public_port)))
    # You only need one result that matches
    result = result.fetchone()
    if result:
        print("RECORD ALREADY EXISTS")
    else:
        data_dict.update({"name": data.name,
                          "id": data.id,
                          "ip": data.ip,
                          "public_port": data.public_port,
                          "layer": data.layer,
                          "contact": data.contact,
                          "date": datetime.datetime.utcnow(),
                          "type": data.type,
                          "uuid": None})
        db.add(user)
        await db.commit()
        await db.refresh(user)
    return jsonable_encoder(data_dict)


@api.get("/user")
async def get_users(db: AsyncSession = Depends(get_db)):
    results = await db.execute(select(User))
    users = results.scalars().all()
    return {"users": users}


@api.get("/user/ids")
async def get_user_ids(db: AsyncSession = Depends(get_db)) -> List:
    results = await db.execute(select(User.id))
    ids = results.scalars().all()
    return list(set(ids))


@api.get("/user/node/{ip}/{public_port}")
async def get_node(ip: str, public_port: int, db: AsyncSession = Depends(get_db)):
    results = await db.execute(select(User).where((User.ip == ip) & (User.public_port == public_port)))
    node = results.scalars().all()
    return {"node": node}


@api.get("/user/ids/{id_}")
async def get_nodes(id_: str, db: AsyncSession = Depends(get_db)):
    results = await db.execute(select(User).where(User.id == id_))
    nodes = results.scalars().all()
    return nodes


@api.get("/user/node/contact/{contact}")
async def get_contact_node_id(contact, db: AsyncSession = Depends(get_db)):
    results = await db.execute(select(User).where(User.contact == contact))
    nodes = results.scalars().all()
    return {contact: nodes}
