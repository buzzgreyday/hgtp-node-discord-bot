import asyncio
import logging
import traceback
from datetime import datetime
import os
from dotenv import load_dotenv
from sqlalchemy.pool import NullPool

from assets.src.database.models import UserModel, NodeModel
from assets.src.schemas import User as UserSchema
from assets.src.schemas import Node as NodeSchema
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession, create_async_engine
from sqlalchemy import select
from fastapi.encoders import jsonable_encoder

load_dotenv()


engine = create_async_engine(
    url=os.getenv("DB_URL"),
    future=True,
    # echo=True,
    poolclass=NullPool,
    connect_args={"server_settings": {"statement_timeout": "9000"}},
)

session = async_sessionmaker(bind=engine, expire_on_commit=False)


class CRUD:
    async def post_user(
        self, data: UserSchema, async_session: async_sessionmaker[AsyncSession]
    ):
        """Creates a new user subscription"""
        async with async_session() as session:
            data.date = datetime.utcnow()
            data_dict = data.dict()
            user = UserModel(**data_dict)
            statement = select(UserModel).where(
                (UserModel.ip == data.ip) & (UserModel.public_port == data.public_port)
            )
            result = await session.execute(statement)
            # You only need one result that matches
            result = result.fetchone()
            if result:
                logging.getLogger(__name__).warning(
                    f"crud.py - The user {data.name} already exists for {data.ip}:{data.public_port}"
                )
            else:
                logging.getLogger(__name__).info(
                    f"crud.py - The user {data.name} ({data.ip}:{data.public_port}) was added to the list of subscribers"
                )
                session.add(user)
                await session.commit()
        return jsonable_encoder(data_dict)

    async def post_data(
        self, data: NodeSchema, async_session: async_sessionmaker[AsyncSession]
    ):
        """Inserts node data from automatic check into database file"""
        async with async_session() as session:
            data_dict = data.dict()
            node_data = NodeModel(**data_dict)
            session.add(node_data)
            try:
                await session.commit()
            except Exception:
                logging.getLogger(__name__).error(
                    f"history.py - localhost error: {traceback.format_exc()}"
                )
                await asyncio.sleep(60)
        return jsonable_encoder(data_dict)

    async def get_user(self, name, async_session: async_sessionmaker[AsyncSession]):
        """Returns a list of all user data"""
        async with async_session() as session:
            results = await session.execute(select(UserModel).where(UserModel.name == name))

        return results.scalars().all()

    async def get_user_ids(
        self, layer: int, async_session: async_sessionmaker[AsyncSession]
    ):
        """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT!!!! Returns a list of all user IDs currently subscribed"""
        list_of_tuples = []
        async with async_session() as session:
            statement = select(UserModel).where(UserModel.layer == layer)
            results = await session.execute(statement)
            ids = results.scalars().all()
            for values in ids:
                list_of_tuples.append((values.id, values.ip, values.public_port))
        return list_of_tuples

    async def get_nodes(
        self,
        id_: str,
        ip: str,
        port: int,
        async_session: async_sessionmaker[AsyncSession],
    ):
        """Return user by ID"""
        async with async_session() as session:
            statement = select(UserModel).where(
                (UserModel.id == id_)
                & (UserModel.ip == ip)
                & (UserModel.public_port == port)
            )
            results = await session.execute(statement)
        return results.scalars().all()

    async def get_node(
        self, ip: str, public_port: int, async_session: async_sessionmaker[AsyncSession]
    ):
        """Return user by IP and port"""
        async with async_session() as session:
            statement = select(UserModel).where(
                (UserModel.ip == ip) & (UserModel.public_port == public_port)
            )
            results = await session.execute(statement)
            node = results.scalars().all()
        return {"node": node}

    async def get_contact_node_id(
        self, contact: str, layer: int, async_session: async_sessionmaker[AsyncSession]
    ):
        """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT!!!! Return user by contact"""
        list_of_tuples = []
        async with async_session() as session:
            results = await session.execute(
                select(UserModel).where(
                    (UserModel.contact == str(contact))
                    & (UserModel.layer == int(layer))
                )
            )
            ids = results.scalars().all()
            for values in ids:
                list_of_tuples.append(
                    (values.id, values.ip, values.public_port, values.layer)
                )
        return list_of_tuples

    async def get_node_data(
        self, ip: str, public_port: int, async_session: async_sessionmaker[AsyncSession]
    ):
        """Return latest node data fetched via automatic check by IP and port"""
        async with async_session() as session:
            statement = (
                select(NodeModel)
                .where(
                    (NodeModel.ip == str(ip))
                    & (NodeModel.public_port == int(public_port))
                )
                .order_by(NodeModel.timestamp_index.desc())
                .limit(1)
            )
            results = await session.execute(statement)
        return results.scalar_one_or_none()

    async def delete_old_entries(self, async_session: async_sessionmaker[AsyncSession]):
        from datetime import timedelta

        async with async_session() as session:
            pass
