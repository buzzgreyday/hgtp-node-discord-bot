import datetime
import logging
import sqlite3
import sys
from typing import List

import sqlalchemy
import uuid as uuid

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select
from fastapi import FastAPI, Depends
from fastapi.encoders import jsonable_encoder

from assets.src import schemas
from assets.src.schemas import User as UserModel
from assets.src.schemas import Node as NodeModel

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

    # uuid: Mapped[str] = mapped_column(default=lambda: str(uuid.uuid4()), index=True, nullable=False, primary_key=True)
    index: Mapped[int] = mapped_column(primary_key=True)
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

    index: Mapped[int] = mapped_column(primary_key=True)
    one_m_system_load_average: Mapped[float] = mapped_column(nullable=True)
    cluster_association_time: Mapped[float] = mapped_column(nullable=True)
    cluster_connectivity: Mapped[str] = mapped_column(nullable=True)
    cluster_dissociation_time: Mapped[float] = mapped_column(nullable=True)
    cluster_name: Mapped[str] = mapped_column(nullable=True)
    cluster_peer_count: Mapped[int] = mapped_column(nullable=True)
    cluster_state: Mapped[str] = mapped_column(nullable=True)
    cluster_version: Mapped[str] = mapped_column(nullable=True)
    contact: Mapped[str] = mapped_column(nullable=True)
    cpu_count: Mapped[int] = mapped_column(nullable=True)
    disk_space_free: Mapped[float] = mapped_column(nullable=True)
    disk_space_total: Mapped[float] = mapped_column(nullable=True)
    former_cluster_association_time: Mapped[float] = mapped_column(nullable=True)
    former_cluster_connectivity: Mapped[str] = mapped_column(nullable=True)
    former_cluster_dissociation_time: Mapped[float] = mapped_column(nullable=True)
    former_cluster_name: Mapped[str] = mapped_column(nullable=True)
    former_cluster_peer_count: Mapped[int] = mapped_column(nullable=True)
    former_cluster_state: Mapped[str] = mapped_column(nullable=True)
    former_reward_state: Mapped[bool] = mapped_column(nullable=True)
    former_timestamp_index: Mapped[datetime.datetime] = mapped_column(nullable=True)
    ip: Mapped[str] = mapped_column(nullable=True)
    id: Mapped[str] = mapped_column(nullable=True)
    last_notified_timestamp: Mapped[datetime.datetime] = mapped_column(nullable=True)
    latest_cluster_session: Mapped[int] = mapped_column(nullable=True)
    latest_version: Mapped[str] = mapped_column(nullable=True)
    layer: Mapped[int] = mapped_column(nullable=True)
    name: Mapped[str] = mapped_column(nullable=True)
    node_cluster_session: Mapped[int] = mapped_column(nullable=True)
    node_peer_count: Mapped[int] = mapped_column(nullable=True)
    wallet_address: Mapped[str] = mapped_column(nullable=True)
    wallet_balance: Mapped[float] = mapped_column(nullable=True)
    notify: Mapped[bool] = mapped_column(nullable=True)
    p2p_port: Mapped[int] = mapped_column(nullable=True)
    public_port: Mapped[int] = mapped_column(nullable=True)
    reward_false_count: Mapped[int] = mapped_column(nullable=True)
    reward_state: Mapped[bool] = mapped_column(nullable=True)
    reward_true_count: Mapped[int] = mapped_column(nullable=True)
    state: Mapped[str] = mapped_column(nullable=True)
    timestamp_index: Mapped[datetime.datetime] = mapped_column(nullable=True)
    version: Mapped[str] = mapped_column(nullable=True)


async def get_db() -> AsyncSession:
    async with SessionLocal() as session:
        yield session

async def get_next_index(Model, db: AsyncSession) -> int:
    # Fetch the last assigned index from the separate table
    result = await db.execute(select(Model.index).order_by(Model.index.desc()).limit(1))
    last_index = result.scalar_one_or_none()
    return 0 if last_index is None else last_index + 1


@api.post("/user/create")
async def post_user(data: UserModel, db: AsyncSession = Depends(get_db)):
    next_index = await get_next_index(User, db)
    data.index = next_index
    data.date = datetime.datetime.utcnow()
    data_dict = data.dict()
    user = User(**data_dict)
    result = await db.execute(select(User).where((User.ip == data.ip) & (User.public_port == data.public_port)))
    # You only need one result that matches
    result = result.fetchone()
    if result:
        print("RECORD ALREADY EXISTS")
    else:
        """data_dict.update(
            {
                "name": data.name,
                "id": data.id,
                "ip": data.ip,
                "public_port": data.public_port,
                "layer": data.layer,
                "contact": data.contact,
                "date": data.date,
                "type": data.type,
                "index": next_index
            }
        )"""

        db.add(user)
        await db.commit()
        await db.refresh(user)
    return jsonable_encoder(data_dict)


@api.post("/data/create")
async def post_data(data: NodeModel, db: AsyncSession = Depends(get_db)):
    next_index = await get_next_index(NodeData, db)
    data.index = next_index
    print(data.index)
    data_dict = data.dict()
    node_data = NodeData(**data_dict)
    """data_dict.update({
        "index": next_index,
        "name": NodeData.name,
        "id": NodeData.id,
        "ip": NodeData.ip,
        "public_port": NodeData.public_port,
        "layer": NodeData.layer,
        "contact": NodeData.contact,
        "cluster_association_time": NodeData.cluster_association_time,
        "cpu_count": NodeData.cpu_count,
        "one_m_system_load_average": NodeData.one_m_system_load_average,
        "disk_space_free": NodeData.disk_space_free,
        "disk_space_total": NodeData.disk_space_total,
        "p2p_port": NodeData.p2p_port,
        "wallet_address": NodeData.wallet_address,
        "wallet_balance": NodeData.wallet_balance,
        "cluster_name": NodeData.cluster_name,
        "former_cluster_name": NodeData.former_cluster_name,
        "state": NodeData.state,
        "cluster_state": NodeData.cluster_state,
        "former_cluster_state": NodeData.former_cluster_state,
        "cluster_connectivity": NodeData.cluster_connectivity,
        "former_cluster_connectivity": NodeData.former_cluster_connectivity,
        "reward_state": NodeData.reward_state,
        "former_reward_state": NodeData.former_reward_state,
        "reward_true_count": NodeData.reward_true_count,
        "reward_false_count": NodeData.reward_false_count,
        "former_cluster_association_time": NodeData.former_cluster_association_time,
        "cluster_dissociation_time": NodeData.cluster_dissociation_time,
        "former_cluster_dissociation_time": NodeData.former_cluster_dissociation_time,
        "node_cluster_session": NodeData.node_cluster_session,
        "latest_cluster_session": NodeData.latest_cluster_session,
        "node_peer_count": NodeData.node_peer_count,
        "cluster_peer_count": NodeData.cluster_peer_count,
        "former_cluster_peer_count": NodeData.former_cluster_peer_count,
        "version": NodeData.version,
        "cluster_version": NodeData.cluster_version,
        "latest_version": NodeData.latest_version,
        "notify": NodeData.notify,
        "last_notified_timestamp": NodeData.last_notified_timestamp,
        "timestamp_index": NodeData.timestamp_index,
        "former_timestamp_index": NodeData.former_timestamp_index
    })"""
    print(data_dict)
    db.add(node_data)
    await db.commit()
    await db.refresh(node_data)
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


@api.get("/user/ids/{id_}")
async def get_nodes(id_: str, db: AsyncSession = Depends(get_db)):
    results = await db.execute(select(User).where(User.id == id_))
    nodes = results.scalars().all()
    return nodes


@api.get("/user/node/{ip}/{public_port}")
async def get_node(ip: str, public_port: int, db: AsyncSession = Depends(get_db)):
    results = await db.execute(select(User).where((User.ip == ip) & (User.public_port == public_port)))
    node = results.scalars().all()
    return {"node": node}


@api.get("/user/node/contact/{contact}")
async def get_contact_node_id(contact, db: AsyncSession = Depends(get_db)):
    results = await db.execute(select(User).where(User.contact == contact))
    nodes = results.scalars().all()
    return {contact: nodes}





@api.get("/data/node/{ip}/{public_port}")
async def get_node_data(ip, public_port, db: AsyncSession = Depends(get_db)):
    print("Fetching API data...")
    results = await db.execute(
                    select(NodeData)
                    .where((NodeData.ip == ip) & (NodeData.public_port == public_port))
                    .order_by(NodeData.timestamp_index.desc())
                    .limit(1))
    node = results.scalar_one_or_none()
    return node
