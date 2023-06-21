from sqlalchemy import Column, String, Integer, Float, DateTime, MetaData
from sqlalchemy.engine import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from fastapi import FastAPI, Depends
from assets.code.schemas import User
# from assets.code.schemas import SQLUserData

engine = create_async_engine("sqlite+aiosqlite:///assets/data/users/db/user.db", connect_args={"check_same_thread": False})
SQLBase = declarative_base()

SessionLocal = async_sessionmaker(engine)

api = FastAPI()


SQLBase.metadata.create_all(engine)


async def get_db():
    async with engine.begin as conn:
        await conn.run_sync(SQLBase.metadata.create_all)
    db = SessionLocal()
    try:
        yield db
    finally:
        await db.close()


@api.post("/user")
async def index(user: User, db: AsyncSession = Depends(get_db)):
    db_user = User.Create(user)
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return


@api.get("/user")
async def get_users(db: AsyncSession = Depends(get_db)):
    users = db.query(User).all()
    return {"users": users}
