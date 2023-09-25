import logging
from datetime import datetime

from models import User, NodeData
from database import engine
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from sqlalchemy import select
from fastapi.encoders import jsonable_encoder

session = async_sessionmaker(bind=engine, expire_on_commit=False)


class CRUD:

    @staticmethod
    async def get_next_index(Model, async_session: async_sessionmaker[AsyncSession]) -> int:
        """Fetch the last assigned index from the separate table"""
        async with async_session() as session:
            statement = select(Model.index).order_by(Model.index.desc()).limit(1)
            result = await session.execute(statement)
            last_index = result.scalar_one_or_none()
            return 0 if last_index is None else last_index + 1

    async def post_user(self, data, async_session: async_sessionmaker[AsyncSession]):
        """Creates a new user subscription"""
        next_index = await CRUD().get_next_index(User, async_sessionmaker[AsyncSession])
        data.index = next_index
        data.date = datetime.utcnow()
        data_dict = data.dict()
        user = User(**data_dict)
        # async with db_lock:
        async with async_session() as session:
            statement = select(User).where((User.ip == data.ip) & (User.public_port == data.public_port))
            result = await session.execute(statement)
            # You only need one result that matches
            result = result.fetchone()
            if result:
                logging.getLogger(__name__).warning(
                    f"crud.py - The user {data.name} already exists for {data.ip}:{data.public_port}")
            else:
                logging.getLogger(__name__).info(
                    f"crud.py - The user {data.name} ({data.ip}:{data.public_port}) was added to the list of subscribers")
                session.add(user)
                await session.commit()
        return jsonable_encoder(data_dict)

