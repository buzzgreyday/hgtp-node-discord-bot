import datetime

import pandas as pd
import uuid as uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select
from fastapi import FastAPI, Depends
from assets.code.schemas import User as UserModel


engine = create_async_engine("sqlite+aiosqlite:///assets/data/db/db.sqlite3", connect_args={"check_same_thread": False})

SessionLocal = async_sessionmaker(engine)

api = FastAPI()


class SQLBase(DeclarativeBase):
    pass


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


async def get_db():
    async with engine.begin() as conn:
        await conn.run_sync(SQLBase.metadata.create_all)
    db = SessionLocal()
    try:
        yield db
    finally:
        await db.close()


@api.post("/user")
async def index(data: UserModel, db: AsyncSession = Depends(get_db)):
    db_user = User(
                   name=data.name,
                   id=data.id,
                   ip=data.ip,
                   public_port=data.public_port,
                   layer=data.layer,
                   contact=data.contact,
                   date=data.date,
                   type=data.type
    )
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user


@api.get("/user")
async def get_all_users(db: AsyncSession = Depends(get_db)):
    results = await db.execute(select(User))
    users = results.scalars().all()
    return {"users": users}


@api.get("/node")
async def get_node(db: AsyncSession = Depends(get_db)):
    results = await db.execute(select(User).where((User.ip == "111.111.111.111") & (User.public_port == 9000)))
    node = results.scalars().all()
    return {"node": node}


@api.get("/nodes")
async def get_nodes(db: AsyncSession = Depends(get_db)):
    results = await db.execute(select(User).where(User.contact == "tester"))
    nodes = results.scalars().all()
    return {"node": nodes}
"""
SELECT *
FROM table1
WHERE EXISTS (SELECT *
              FROM table2
              WHERE Lead_Key = @Lead_Key
                        AND table1.CM_PLAN_ID = table2.CM_PLAN_ID
                        AND table1.Individual_ID = table2.Individual_ID)
"""
