import asyncio
import logging
import traceback
from datetime import datetime, timezone
import os

from dotenv import load_dotenv
from assets.src.database.models import (
    UserModel,
    NodeModel,
    OrdinalModel,
    PriceModel,
    RewardStatsModel, MetricStatsModel,
)
from assets.src.schemas import (
    User as UserSchema,
    PriceSchema,
    OrdinalSchema,
    RewardStatsSchema, MetricStatsSchema,
)
from assets.src.schemas import Node as NodeSchema
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession, create_async_engine
from sqlalchemy import select, delete, update, desc
from fastapi.encoders import jsonable_encoder

load_dotenv()


database_url = os.getenv("DB_URL")

# Create the database engine
engine = create_async_engine(
    database_url,
    future=True,
    # echo=True
    # poolclass=NullPool,
)


class DatabaseBatchProcessor:
    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.batch_data = []

    async def process_batch(self, async_session: async_sessionmaker[AsyncSession]):
        if self.batch_data:
            async with async_session() as session:
                for data in self.batch_data:
                    session.add(data)
                await session.commit()
            self.batch_data = []

    async def add_to_batch(self, data, async_session: async_sessionmaker[AsyncSession]):
        self.batch_data.append(data)
        if len(self.batch_data) >= self.batch_size:
            await self.process_batch(async_session)


batch_processor = DatabaseBatchProcessor(batch_size=100)


class CRUD:
    async def post_user(
        self, data: UserSchema, async_session: async_sessionmaker[AsyncSession]
    ):
        """Creates a new user subscription"""
        async with async_session() as session:
            data.date = datetime.now(timezone.utc)
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
        await batch_processor.add_to_batch(OrdinalModel(**data.__dict__), async_session)
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

    async def post_reward_stats(
        self, data: RewardStatsSchema, async_session: async_sessionmaker[AsyncSession]
    ):
        """Post statistical data row by row"""
        async with async_session() as session:
            stat_data = RewardStatsModel(**data.__dict__)
            # Create a StatModel instance for each row of data
            session.add(stat_data)
            await session.commit()
            logging.getLogger("stats").debog(f"crud.py - Stats post: SUCCESS!")
        return jsonable_encoder(stat_data)

    async def update_reward_stats(
        self, data: RewardStatsSchema, async_session: async_sessionmaker[AsyncSession]
    ):
        """Update statistical data"""

        async with async_session() as session:
            try:
                await session.execute(
                    update(RewardStatsModel)
                    .where(RewardStatsModel.destinations == data.destinations)
                    .values(**data.__dict__)
                )
                await session.commit()
                logging.getLogger("stats").debug(f"crud.py - Reward stats update: SUCCESS!")
            except Exception as e:
                logging.getLogger("stats").error(f"crud.py - Reward stats update: FAIL!\n{traceback.format_exc()}")

    async def post_metric_stats(
        self, data: MetricStatsSchema, async_session: async_sessionmaker[AsyncSession]
    ):
        """Post statistical data row by row"""
        async with async_session() as session:
            metric_data = MetricStatsModel(**data.__dict__)
            # Create a StatModel instance for each row of data
            session.add(metric_data)
            await session.commit()
            logging.getLogger("stats").error(f"crud.py - Metric stats post: SUCCESS!")
        return jsonable_encoder(metric_data)

    async def update_metric_stats(
        self, data: MetricStatsSchema, async_session: async_sessionmaker[AsyncSession]
    ):
        """Update statistical data based on hash_index"""

        async with async_session() as session:
            try:
                await session.execute(
                    update(MetricStatsModel)
                    .where(MetricStatsModel.hash_index == data.hash_index)
                    .values(**data.__dict__)
                )
                await session.commit()
                logging.getLogger("stats").debug(f"crud.py - Metric stats update: SUCCESS!")
            except Exception:
                logging.getLogger("stats").error(f"crud.py - Metric stats update: FAIL!\n{traceback.format_exc()}")

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

    async def delete_db_ordinal(
        self, ordinal, async_session: async_sessionmaker[AsyncSession]
    ):
        """Delete the user subscription based on name, ip, port"""

        async with async_session() as session:
            statement = delete(OrdinalModel).where((OrdinalModel.ordinal == ordinal))
            await session.execute(statement)
            await session.commit()
            logging.getLogger("rewards").warning(
                f"crud.py - deleted ordinal {ordinal} to avoid duplicates"
            )
            return

    async def get_html_page_stats(
        self,
        request,
        templates,
        dag_address,
        async_session: async_sessionmaker[AsyncSession],
    ):

        async with async_session() as session:
            reward_results = await session.execute(
                select(RewardStatsModel).where(
                    RewardStatsModel.destinations == dag_address
                )
            )
            metric_results = await session.execute(
                select(MetricStatsModel).where(
                    MetricStatsModel.destinations == dag_address
                )
            )
            reward_results = reward_results.scalar_one_or_none()
            metric_results = metric_results.fetchall()
            logging.getLogger("stats").critical(traceback.format_exc())
            metric_dicts = []
            for node_metrics in metric_results:
                metric_dicts.append(node_metrics[0].__dict__)
            metric_dicts = sorted(metric_dicts, key=lambda d: d["layer"])
            dag_address = reward_results.destinations
            earner_score = reward_results.earner_score
            count = reward_results.count
            percent_earning_more = reward_results.percent_earning_more
            dag_address_sum = reward_results.dag_address_sum
            dag_median_sum = reward_results.dag_median_sum
            dag_address_daily_mean = reward_results.dag_address_daily_mean
            usd_address_sum = reward_results.usd_address_sum
            usd_address_daily_sum = reward_results.usd_address_daily_sum
            daily_dag_estimation_low = (
                reward_results.dag_address_daily_mean - reward_results.dag_daily_std_dev
            )
            daily_dag_estimation_high = (
                reward_results.dag_address_daily_mean + reward_results.dag_daily_std_dev
            )
            dag_address_daily_std_dev = (
                f"{round(daily_dag_estimation_low)} - {round(daily_dag_estimation_high)}"
            )
            monthly_dag_average = dag_address_daily_mean * 30
            from assets.src.database.database import get_latest_db_price
            price_timestamp, price_dagusd = await get_latest_db_price()
            price_dagusd = 0 if price_dagusd is None else price_dagusd
            price_timestamp = "ERROR!" if price_timestamp is None else price_timestamp
            if price_dagusd != 0.000000000:
                dag_earnings_price_now = dag_address_sum * price_dagusd
            if reward_results.dag_address_sum_dev > 0:
                dag_address_sum_dev = f"+{round(reward_results.dag_address_sum_dev)}"
            else:
                dag_address_sum_dev = round(reward_results.dag_address_sum_dev)
            # Sum of all $DAG minted, minus very high earning wallets (Stardust Collective wallet, etc.)
            dag_minted_for_validators = reward_results.nonoutlier_dag_addresses_minted_sum
            percent_of_nonoutlier_validator_pool = (reward_results.dag_address_sum / dag_minted_for_validators) * 100
            # Highest earning address, minus very high earning wallets (Stardust Collective wallet, etc.)
            dag_highest_earning = reward_results.above_dag_address_earner_highest
            # What addresses earning more are earning on average
            above_dag_earnings_mean = reward_results.above_dag_addresses_earnings_mean
            # What the address is missing out on (average)
            above_dag_address_deviation_from_mean = reward_results.above_dag_address_earnings_deviation_from_mean
            # What the address is missing out on (compared to highest earning address)
            above_dag_address_deviation_from_highest_earning = reward_results.above_dag_address_earnings_from_highest
            above_dag_address_std_dev = reward_results.above_dag_address_earnings_std_dev
            # What those addresses earning more is earning (standard deviation)
            above_dag_address_std_dev_high = above_dag_earnings_mean + above_dag_address_std_dev
            above_dag_address_std_dev_low = above_dag_earnings_mean - above_dag_address_std_dev

            content = templates.TemplateResponse(
                    "index.html",
                    dict(request=request,
                         dag_address=dag_address,
                         earner_score=earner_score,
                         count=count,
                         percent_earning_more=round(percent_earning_more, 2),
                         dag_address_sum=round(dag_address_sum, 2),
                         dag_address_sum_dev=dag_address_sum_dev,
                         dag_median_sum=round(dag_median_sum, 2),
                         dag_address_daily_mean=round(dag_address_daily_mean, 2),
                         dag_address_daily_std_dev=dag_address_daily_std_dev,
                         dag_address_monthly_mean=round(monthly_dag_average, 2),
                         dag_price_now=round(price_dagusd, 4),
                         dag_price_now_timestamp=datetime.fromtimestamp(price_timestamp),
                         dag_earnings_price_now=round(dag_earnings_price_now, 2),
                         usd_address_sum=round(usd_address_sum, 2),
                         usd_address_daily_sum=round(usd_address_daily_sum, 2),
                         rewards_plot_path=f"rewards_{dag_address}.html",
                         cpu_plot_path=f"cpu_{dag_address}.html",

                         dag_minted_for_validators=round(dag_minted_for_validators, 2),
                         dag_highest_earner=round(dag_highest_earning, 2),
                         above_dag_earnings_mean=round(above_dag_earnings_mean, 2),
                         above_dag_address_deviation_from_mean=round(above_dag_address_deviation_from_mean, 2),
                         above_dag_address_deviation_from_highest_earning=round(above_dag_address_deviation_from_highest_earning, 2),
                         above_dag_address_std_dev_high=round(above_dag_address_std_dev_high, 2),
                         above_dag_address_std_dev_low=round(above_dag_address_std_dev_low, 2),
                         percent_of_nonoutlier_validator_pool=round(percent_of_nonoutlier_validator_pool, 2),

                         metric_dicts=metric_dicts)
            )
            if reward_results:
                return content


    async def get_latest_db_price(
        self, async_session: async_sessionmaker[AsyncSession]
    ):
        """Get the latest ordinal data existing in the database"""
        async with async_session() as session:
            statement = (
                select(PriceModel).order_by(PriceModel.timestamp.desc()).limit(1)
            )
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

    async def get_timestamp_db_price(
        self, ordinal_timestamp: int, async_session: async_sessionmaker[AsyncSession]
    ):
        """Get the latest ordinal data existing in the database"""
        async with async_session() as session:
            statement = (
                select(PriceModel)
                .filter(PriceModel.timestamp <= ordinal_timestamp)
                .order_by(desc(PriceModel.timestamp))
                .limit(1)
            )
            results = await session.execute(statement)
            timestamp_price_data = results.scalar()
        if timestamp_price_data:
            logging.getLogger("rewards").info(
                f"crud.py - success requesting database timestamp price: {timestamp_price_data.timestamp, timestamp_price_data.usd}"
            )
            return timestamp_price_data.timestamp, timestamp_price_data.usd
        else:
            logging.getLogger("rewards").warning(
                f"crud.py - failed requesting database timestamp price"
            )
            return

    async def get_latest_db_ordinal(
        self, async_session: async_sessionmaker[AsyncSession]
    ):
        """Get the latest ordinal data existing in the database"""
        async with async_session() as session:
            statement = (
                select(OrdinalModel).order_by(OrdinalModel.ordinal.desc()).limit(1)
            )
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

    async def get_ordinals_data_from_timestamp(
        self, timestamp: int, async_session: async_sessionmaker[AsyncSession]
    ):
        async with async_session() as session:
            batch_size = 200000
            offset = 0
            data = {
                "timestamp": [],
                "ordinals": [],
                "destinations": [],
                "dag": [],
                "usd_per_token": [],
            }
            while True:
                try:
                    statement = (
                        select(
                            OrdinalModel.timestamp,
                            OrdinalModel.ordinal,
                            OrdinalModel.destination,
                            OrdinalModel.amount,
                            OrdinalModel.usd,
                        )
                        .filter(OrdinalModel.timestamp >= timestamp)
                        .offset(offset)
                        .limit(batch_size)
                    )
                    logging.getLogger("stats").debug(f"Get ordinals from timestamp: {timestamp}, offset: {offset}")
                    results = await session.execute(statement)
                    batch_results = results.fetchall()
                except Exception:
                    logging.getLogger("stats").warning(traceback.format_exc())

                if not batch_results:
                    logging.getLogger("stats").debug(f"Got all ordinals!")
                    break  # No more data

                for row in batch_results:
                    data["timestamp"].append(row.timestamp)
                    data["ordinals"].append(row.ordinal)
                    data["destinations"].append(row.destination)
                    data["dag"].append(row.amount)
                    data["usd_per_token"].append(row.usd)

                offset += batch_size
                # await asyncio.sleep(3)

        return data

    async def get_historic_node_data_from_timestamp(
        self, timestamp: int, async_session: async_sessionmaker[AsyncSession]
    ):
        """
        Get timeslice data from the node database.
        """
        one_gigabyte = 1073741824
        async with async_session() as session:
            batch_size = 200000
            offset = 0
            data = {
                "timestamp": [],
                "destinations": [],
                "layer": [],
                "ip": [],
                "id": [],
                "public_port": [],
                "cpu_load_1m": [],
                "cpu_count": [],
                "disk_free": [],
                "disk_total": [],
            }
            timestamp_datetime = datetime.fromtimestamp(timestamp)

            while True:
                statement = (
                    select(
                        NodeModel.timestamp_index,
                        NodeModel.wallet_address,
                        NodeModel.layer,
                        NodeModel.ip,
                        NodeModel.id,
                        NodeModel.public_port,
                        NodeModel.one_m_system_load_average,
                        NodeModel.cpu_count,
                        NodeModel.disk_space_free,
                        NodeModel.disk_space_total,
                        NodeModel.last_known_cluster_name,
                    )
                    .filter(NodeModel.timestamp_index >= timestamp_datetime)
                    .offset(offset)
                    .limit(batch_size)
                )
                logging.getLogger("stats").debug(f"Get node_data from timestamp: {timestamp}, offset: {offset}")
                results = await session.execute(statement)
                batch_results = results.fetchall()

                if not batch_results:
                    logging.getLogger("stats").debug("All node_data batches processed")
                    break  # No more data

                for row in batch_results:
                    if row.last_known_cluster_name == "mainnet":
                        data["timestamp"].append(round(row.timestamp_index.timestamp()))
                        data["destinations"].append(row.wallet_address)
                        data["layer"].append(row.layer)
                        data["ip"].append(row.ip)
                        data["id"].append(row.id)
                        data["public_port"].append(row.public_port)
                        data["cpu_load_1m"].append(row.one_m_system_load_average)
                        data["cpu_count"].append(row.cpu_count)
                        try:
                            data["disk_free"].append(row.disk_space_free / one_gigabyte)
                            data["disk_total"].append(
                                row.disk_space_total / one_gigabyte
                            )
                        except ZeroDivisionError:
                            data["disk_free"].append(0.0)
                            data["disk_total"].append(0.0)

                offset += batch_size
                # await asyncio.sleep(3)

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
