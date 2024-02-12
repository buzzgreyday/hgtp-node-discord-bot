import asyncio
import logging
import traceback
from datetime import datetime
import os

from dotenv import load_dotenv
from assets.src.database.models import UserModel, NodeModel, OrdinalModel, PriceModel, StatModel
from assets.src.schemas import User as UserSchema, PriceSchema, OrdinalSchema, StatSchema
from assets.src.schemas import Node as NodeSchema
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession, create_async_engine
from sqlalchemy import select, delete, update
from fastapi.encoders import jsonable_encoder

load_dotenv()


database_url = os.getenv("DB_URL")

# Create the database engine
engine = create_async_engine(
    database_url,
    future=True,
    # echo=True,
    # poolclass=NullPool,
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
                logging.getLogger("app").warning(
                    f"crud.py - The user {data.name} already exists for {data.ip}:{data.public_port}"
                )
            else:
                logging.getLogger("app").info(
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
                logging.getLogger("app").error(
                    f"crud.py - localhost error: {traceback.format_exc()}"
                )
                await asyncio.sleep(60)
        return jsonable_encoder(data_dict)

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
                logging.getLogger("rewards").error(
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
                logging.getLogger("rewards").error(
                    f"crud.py - localhost error: {traceback.format_exc()}"
                )
        return jsonable_encoder(data)

    async def post_stats(
            self, data: StatSchema, async_session: async_sessionmaker[AsyncSession]
    ):
        """Post statistical data row by row"""
        async with async_session() as session:
            stat_data = StatModel(**data.__dict__)
            # Create a StatModel instance for each row of data
            session.add(stat_data)
            await session.commit()
            logging.getLogger("rewards").error(
                f"crud.py - Stats post: SUCCESS!"
            )
        return jsonable_encoder(stat_data)


    async def update_stats(self, data: StatSchema, async_session: async_sessionmaker[AsyncSession]):
        """Update statistical data"""

        async with async_session() as session:
            try:
                await session.execute(
                    update(StatModel)
                    .where(StatModel.destinations == data.destinations)
                    .values(**data.__dict__)
                )
                await session.commit()
                logging.getLogger("rewards").error(f"crud.py - Stats update: SUCCESS!")
            except:
                print("Stats update: FAILED!\n", traceback.format_exc())
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
        """
        Placeholder for automatic deletion of database entries older than x.
        Beware: just passes entries without functionality
        """
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
            logging.getLogger("rewards").warning(
                f"crud.py - deleted ordinal {ordinal} to avoid duplicates"
            )
            return

    async def get_html_page_stats(self, request, templates, dag_address, async_session: async_sessionmaker[AsyncSession]
    ):
        async with async_session() as session:
            results = await session.execute(
                select(StatModel).where(StatModel.destinations == dag_address)
            )

        results = results.scalar_one_or_none()
        print(f"http://localhost:8000/static/{results.destinations}.jpg")
        if results:
            print(results.dag_address_daily_mean)
            return templates.TemplateResponse("index.html",
                                              {"request": request,
                                               "dag_address": results.destinations,
                                               "daily_effectivity_score": results.daily_effectivity_score,
                                               "effectivity_score": results.effectivity_score,
                                               "earner_score": results.earner_score,
                                               "count": results.count,
                                               "percent_earning_more": round(results.percent_earning_more, 2),
                                               "dag_address_sum": round(results.dag_address_sum, 2),
                                               "dag_address_sum_dev": round(results.dag_address_sum_dev, 2),
                                               "dag_median_sum": round(results.dag_median_sum, 2),
                                               "dag_address_daily_sum_dev": round(results.dag_address_daily_sum_dev, 2),
                                               "dag_address_daily_mean": round(results.dag_address_daily_mean, 2),
                                               "dag_address_daily_std_dev": round(results.dag_daily_std_dev, 2),
                                               "usd_address_sum": results.usd_address_sum,
                                               "usd_address_daily_sum": results.usd_address_daily_sum,
                                               "plot_path": f"http://localhost:8000/static/{results.destinations}.jpg"})
        else:
            print("Error")


    async def get_latest_db_price(self,
            async_session: async_sessionmaker[AsyncSession]
    ):
        """Get the latest ordinal data existing in the database"""
        async with async_session() as session:
            statement = select(PriceModel).order_by(PriceModel.timestamp.desc()).limit(1)
            results = await session.execute(statement)
            latest_price_data = results.scalar()
        if latest_price_data:
            logging.getLogger("rewards").info(
                f"crud.py - success requesting database latest price: {latest_price_data.timestamp, latest_price_data.usd}"
            )
            return latest_price_data.timestamp, latest_price_data.usd
        else:
            logging.getLogger("rewards").warning(
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
            logging.getLogger("rewards").info(
                f"crud.py - success requesting database latest ordinal: {latest_ordinal_data.ordinal}"
            )
            return latest_ordinal_data.timestamp, latest_ordinal_data.ordinal
        else:
            logging.getLogger("rewards").warning(
                f"crud.py - failed requesting database latest ordinal"
            )
            return

    async def get_ordinals_data_from_timestamp(self, timestamp: int, async_session: async_sessionmaker[AsyncSession]):
        """
        Get timeslice data from the ordinal database.
        Beware: the database usd column is per token, not the sum of the token value.
        """
        async with async_session() as session:
            statement = select(OrdinalModel).filter(OrdinalModel.timestamp >= int(timestamp)).order_by(OrdinalModel.destination)
            results = await session.execute(statement)
            results = results.scalars().all()
            # Extract columns into separate lists
            data = {
                'timestamp': [],
                'ordinals': [],
                'destinations': [],
                'dag': [],
                'usd_per_token': []
            }
            for row in results:
                data['timestamp'].append(row.timestamp)
                data['ordinals'].append(row.ordinal)
                data['destinations'].append(row.destination)
                data['dag'].append(row.amount)
                data['usd_per_token'].append(row.usd)

            return data
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
                    (UserModel.discord == str(contact))
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
