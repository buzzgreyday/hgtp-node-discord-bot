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
    async def get_next_index(Model, session) -> int:
        """Fetch the last assigned index from the separate table"""
        statement = select(Model.index).order_by(Model.index.desc()).limit(1)
        result = await session.execute(statement)
        last_index = result.scalar_one_or_none()
        return 0 if last_index is None else last_index + 1

    async def post_user(self, data: User, async_session: async_sessionmaker[AsyncSession]):
        """Creates a new user subscription"""
        async with async_session() as session:
            next_index = await CRUD().get_next_index(User, session)
            data.index = next_index
            data.date = datetime.utcnow()
            data_dict = data.dict()
            user = User(**data_dict)
            # async with db_lock:
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

    async def post_data(self, data: NodeData, async_session: async_sessionmaker[AsyncSession]):
        """Inserts node data from automatic check into database file"""
        next_index = await get_next_index(NodeData, db)
        data.index = next_index
        data_dict = data.dict()
        node_data = NodeData(**data_dict)
        # async with db_lock:
        db.add(node_data)
        while True:
            try:
                await db.commit()
            except (sqlalchemy.exc.OperationalError, sqlite3.OperationalError):
                logging.getLogger(__name__).info(
                    f"database.py - Operational error while posting node data  {data.name} ({data.ip}:{data.public_port}, {data.last_known_cluster_name})")
                await asyncio.sleep(1)
            else:
                break
        await db.refresh(node_data)
        await db.close()
        return jsonable_encoder(data_dict)

