import logging
from datetime import datetime

from models import UserModel, NodeModel
from assets.src.schemas import User as UserSchema
from assets.src.schemas import Node as NodeSchema
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

    async def post_user(self, data: UserSchema, async_session: async_sessionmaker[AsyncSession]):
        """Creates a new user subscription"""
        async with async_session() as session:
            # next_index = await self.get_next_index(UserModel, session)
            # data.index = next_index
            data.date = datetime.utcnow()
            data_dict = data.dict()
            user = UserModel(**data_dict)
            statement = select(UserModel).where((UserModel.ip == data.ip) & (UserModel.public_port == data.public_port))
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

    async def post_data(self, data: NodeSchema, async_session: async_sessionmaker[AsyncSession]):
        """Inserts node data from automatic check into database file"""
        async with async_session() as session:
            # next_index = await self.get_next_index(NodeModel, session)
            # data.index = next_index
            data_dict = data.dict()
            node_data = NodeModel(**data_dict)
            session.add(node_data)
            await session.commit()
            return jsonable_encoder(data_dict)

    async def get_users(self, async_session:async_sessionmaker[AsyncSession]):
        """Returns a list of all user data"""
        async with async_session() as session:
            results = await session.execute(select(UserModel))
            users = results.scalars().all()
            return {"users": users}

    async def get_user_ids(self, layer: int, async_session: async_sessionmaker[AsyncSession]):
        """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT!!!! Returns a list of all user IDs currently subscribed"""
        list_of_tuples = []
        async with async_session() as session:
            statement = select(UserModel).where(UserModel.layer == layer)
            results = await session.execute(statement)
            ids = results.scalars().all()
            for values in ids:
                list_of_tuples.append((values.id, values.ip, values.public_port))
            return list_of_tuples

    async def get_nodes(self, id_: str, ip: str, port: int, async_session: async_sessionmaker[AsyncSession]):
        """Return user by ID"""
        async with async_session() as session:
            statement = select(UserModel).where((UserModel.id == id_) & (UserModel.ip == ip) & (UserModel.public_port == port))
            results = await session.execute(statement)
            return results.scalars().all()

    async def get_node(self, ip: str, public_port: int, async_session: async_sessionmaker[AsyncSession]):
        """Return user by IP and port"""
        async with async_session() as session:
            statement = select(UserModel).where((UserModel.ip == ip) & (UserModel.public_port == public_port))
            results = await session.execute(statement)
            node = results.scalars().all()
            return {"node": node}

    async def get_contact_node_id(self, contact, layer, async_session: async_sessionmaker[AsyncSession]):
        """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT!!!! Return user by contact"""
        list_of_tuples = []
        async with async_session() as session:
            results = await session.execute(select(UserModel).where((UserModel.contact == contact) & (UserModel.layer == layer)))
            ids = results.scalars().all()
            for values in ids:
                list_of_tuples.append((values.id, values.ip, values.public_port, values.layer))
            return list_of_tuples

    async def get_node_data(self, ip, public_port, async_session: async_sessionmaker[AsyncSession]):
        """Return latest node data fetched via automatic check by IP and port"""
        async with async_session() as session:
            statement = select(NodeModel).where((NodeModel.ip == ip) & (NodeModel.public_port == public_port)).order_by(NodeModel.timestamp_index.desc()).limit(1)
            results = await session.execute(statement)
            await session.close()
            return results.scalar_one_or_none()

