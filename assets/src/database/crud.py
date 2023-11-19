import asyncio
import logging
import traceback
from datetime import datetime
import os
from dotenv import load_dotenv
from sqlalchemy.pool import NullPool
from sqlalchemy import select, delete, desc, distinct

from assets.src import schemas
from assets.src.database.models import UserModel, NodeModel, OrdinalModel, PriceModel
from assets.src.schemas import User as UserSchema, PriceSchema, OrdinalSchema
from assets.src.schemas import Node as NodeSchema
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession, create_async_engine
from sqlalchemy import select, delete
from fastapi.encoders import jsonable_encoder

load_dotenv()


engine = create_async_engine(
    url=os.getenv("DB_URL"),
    future=True,
    # echo=True,
    # poolclass=NullPool,
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
            results = await session.execute(
                select(UserModel).where(UserModel.name == name)
            )

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

    async def delete_user_entry(
        self, data: UserModel, async_session: async_sessionmaker[AsyncSession]
    ):
        """Delete the user subscription based on name, ip, port"""

        async with async_session() as session:
            statement = delete(UserModel).where(
                (UserModel.ip == data.ip) & (UserModel.public_port == data.public_port)
            )
            await session.execute(statement)
            await session.commit()

    async def delete_old_entries(self, async_session: async_sessionmaker[AsyncSession]):
        from datetime import timedelta

        async with async_session() as session:
            pass

    async def delete_db_ordinal(self, ordinal, async_session: async_sessionmaker[AsyncSession]):
        """Delete the user subscription based on name, ip, port"""

        async with async_session() as session:
            statement = delete(OrdinalModel).where(
                (OrdinalModel.ordinal == ordinal)
            )
            await session.execute(statement)
            await session.commit()
            logging.getLogger(__name__).warning(
                f"crud.py - deleted ordinal {ordinal} to avoid duplicates"
            )
            return

    async def post_ordinal(
            self, data: OrdinalSchema, async_session: async_sessionmaker[AsyncSession]
    ):
        """Inserts node data from automatic check into database file"""
        async with async_session() as session:
            data = OrdinalModel(**data.__dict__)
            session.add(data)
            try:
                await session.commit()
            except Exception:
                logging.getLogger(__name__).error(
                    f"crud.py - localhost error: {traceback.format_exc()}"
                )
                await asyncio.sleep(60)
        return jsonable_encoder(data)

    async def post_prices(
            self, data: PriceSchema, async_session: async_sessionmaker[AsyncSession]
    ):
        """Inserts node data from automatic check into database file"""
        async with async_session() as session:
            data = PriceModel(**data.__dict__)
            session.add(data)
            try:
                await session.commit()
            except Exception:
                logging.getLogger(__name__).error(
                    f"crud.py - localhost error: {traceback.format_exc()}"
                )
        return jsonable_encoder(data)

    async def get_timestamp_db_price(self,
            ordinal_timestamp: int, async_session: async_sessionmaker[AsyncSession]
    ):
        """Get the latest ordinal data existing in the database"""
        async with async_session() as session:
            statement = select(PriceModel).filter(PriceModel.timestamp <= ordinal_timestamp).order_by(
                desc(PriceModel.timestamp)).limit(1)
            results = await session.execute(statement)
            timestamp_price_data = results.scalar()
        if timestamp_price_data:
            logging.getLogger(__name__).info(
                f"crud.py - success requesting database timestamp price: {timestamp_price_data.timestamp, timestamp_price_data.usd}"
            )
            return timestamp_price_data.timestamp, timestamp_price_data.usd
        else:
            logging.getLogger(__name__).warning(
                f"crud.py - failed requesting database timestamp price"
            )
            return

    async def get_latest_db_price(self,
            async_session: async_sessionmaker[AsyncSession]
    ):
        """Get the latest ordinal data existing in the database"""
        async with async_session() as session:
            statement = select(PriceModel).order_by(PriceModel.timestamp.desc()).limit(1)
            results = await session.execute(statement)
            latest_price_data = results.scalar()
        if latest_price_data:
            logging.getLogger(__name__).info(
                f"crud.py - success requesting database latest price: {latest_price_data.timestamp, latest_price_data.usd}"
            )
            return latest_price_data.timestamp, latest_price_data.usd
        else:
            logging.getLogger(__name__).warning(
                f"crud.py - failed requesting database latest price"
            )
            return

    async def get_latest_db_ordinal(self,
            async_session: async_sessionmaker[AsyncSession]
    ):
        """Get the latest ordinal data existing in the database"""
        async with async_session() as session:
            statement = select(OrdinalModel).order_by(OrdinalModel.ordinal.desc()).limit(1)
            results = await session.execute(statement)
            latest_ordinal_data = results.scalar()

        if latest_ordinal_data:
            logging.getLogger(__name__).info(
                f"crud.py - success requesting database latest ordinal: {latest_ordinal_data.ordinal}"
            )
            return latest_ordinal_data.timestamp, latest_ordinal_data.ordinal
        else:
            logging.getLogger(__name__).warning(
                f"crud.py - failed requesting database latest ordinal"
            )
            return
