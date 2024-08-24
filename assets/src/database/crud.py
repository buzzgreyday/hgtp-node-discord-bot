import asyncio
import gc
import logging
import traceback
from datetime import datetime, timedelta
import os
from typing import List, Dict

import aiohttp.client_exceptions
import numpy as np
import pandas as pd
import pydantic
from dotenv import load_dotenv
from assets.src.database.models import (
    UserModel,
    NodeModel,
    OrdinalModel,
    PriceModel,
    RewardStatsModel, MetricStatsModel, OldNodeModel, OldOrdinalModel
)
from assets.src.schemas import (
    User as UserSchema,
    PriceSchema,
    OrdinalSchema,
    RewardStatsSchema, MetricStatsSchema
)

from assets.src.schemas import Node as NodeSchema
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession, create_async_engine
from sqlalchemy import select, delete, update, desc, and_
from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError

load_dotenv()

database_url = os.getenv("DB_URL")

# Create the database engine
engine = create_async_engine(
    database_url,
    future=True,
    pool_size=40,
    max_overflow=40,
    pool_timeout=60,
    pool_pre_ping=True,
    pool_recycle=3600
    # echo=True,
    # poolclass=NullPool,
)


class DatabaseBatchProcessor:
    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.batch_data = []

    async def process_batch(self, async_session: async_sessionmaker[AsyncSession]):
        if self.batch_data:
            async with async_session() as session:
                try:
                    for data in self.batch_data:
                        session.add(data)
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    raise e
                finally:
                    self.batch_data = []

    async def add_to_batch(self, data: NodeModel | OldNodeModel | OrdinalModel, async_session: async_sessionmaker[AsyncSession]):
        self.batch_data.append(data)
        if len(self.batch_data) >= self.batch_size:
            await self.process_batch(async_session)


class CRUD:

    async def post_user(
            self, data: UserSchema, async_session: async_sessionmaker[AsyncSession]
    ):
        """Creates a new user subscription"""
        async with async_session() as session:
            data.date = datetime.now()
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
        batch_processor = DatabaseBatchProcessor(batch_size=100)
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
            logging.getLogger("stats").debug(f"crud.py - Stats post: SUCCESS!")
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

    async def update_user(
            self, cached_subscriber: Dict, async_session: async_sessionmaker[AsyncSession]
    ):
        """Update user data based on cache"""
        if cached_subscriber["removal_datetime"]:
            removal_datetime = pd.to_datetime(cached_subscriber["removal_datetime"])
        else:
            removal_datetime = None
        async with async_session() as session:
            try:
                await session.execute(
                    update(UserModel)
                    .where(and_(UserModel.ip == cached_subscriber["ip"],
                                UserModel.public_port == cached_subscriber["public_port"],
                                UserModel.id == cached_subscriber["id"], UserModel.layer == cached_subscriber["layer"]))
                    .values(cluster=cached_subscriber["cluster_name"], removal_datetime=removal_datetime)
                )
                await session.commit()
                logging.getLogger("app").debug(f"crud.py - User cache update: SUCCESS!")
            except Exception:
                logging.getLogger("app").error(f"crud.py - User cache update: FAIL!\n{traceback.format_exc()}")

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

    async def delete_rows_not_in_new_data(self, data: list[dict], async_session: async_sessionmaker[AsyncSession]):
        async with async_session() as session:
            hashes = {record["hash_index"] for record in data}
            await session.execute(delete(MetricStatsModel).where(MetricStatsModel.hash_index.not_in(hashes)))
            await session.commit()

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
                logging.getLogger("stats").debug(
                    f"Message: Metric stats update successful\n"
                    f"Module: assets/src/database/crud.py\n"
                )
            except Exception as e:
                logging.getLogger("stats").error(
                    f"Message: Metric stats update failed\n"
                    f"Module: assets/src/database/crud.py\n"
                    f"Type: {e}\n"
                    f"Details: {traceback.format_exc()}\n")

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

    async def _get_data_ids(self, async_session, batch_size=10000, offset=None, cutoff_date=datetime.now() - timedelta(days=32)):
        async with async_session() as session:
            results = await session.execute(
                select(NodeModel).filter(NodeModel.timestamp_index < cutoff_date).offset(offset).limit(batch_size))
            results = results.scalars().all()
        return results

    async def _migrate_data_ids(self, batch_results, processed_ids, async_session, batch_processor=None):
        # Batch processing
        for data in batch_results:
            data = NodeSchema(**data.__dict__)
            try:
                await batch_processor.add_to_batch(OldNodeModel(**data.__dict__), async_session)
            except IntegrityError as e:
                logging.getLogger("db_optimization").error(
                    f"Message: Node data already present in the old_data table\n"
                    f"Module: assets/src/database/crud.py\n"
                    f"Type: {e}\n"
                    f"Details: {traceback.format_exc()}")
            finally:
                processed_ids.add(data.index)
        return processed_ids

    async def _delete_data_ids(self, processed_ids, async_session):
        # Deleting old data after processing current batch
        try:
            async with async_session() as session:
                if processed_ids:
                    await session.execute(
                        delete(NodeModel).filter(NodeModel.index.in_(processed_ids))
                    )
                    await session.commit()
        except Exception as e:
            logging.getLogger("db_optimization").error(f"Message: Something happened during node data deletion.\n"
                                                       f"Module: assets/src/database/crud.py\n"
                                                       f"Type: {e}\n"
                                                       f"Details: {traceback.format_exc()}")

    async def migrate_old_data(self, async_session: async_sessionmaker[AsyncSession], configuration):
        """
        Placeholder for automatic migration of database entries older than x.
        Beware: just passes entries without functionality
        """
        logging.getLogger("db_optimization").info(f"Node data migration initiated.")
        batch_size = 10000
        batch_processor = DatabaseBatchProcessor(batch_size)
        offset = 0

        # Query for old data
        while True:
            processed_ids = set()
            batch_results = await self._get_data_ids(async_session, batch_size=batch_size, offset=offset, cutoff_date=datetime.now() - timedelta(days=int(configuration["general"]["save data (days)"])))

            if not batch_results:
                logging.getLogger("db_optimization").info(f"No more node data to migrate")
                break  # No more data

            processed_ids = await self._migrate_data_ids(batch_results, processed_ids, async_session, batch_processor=batch_processor)
            await self._delete_data_ids(processed_ids, async_session)

            del batch_results
            del processed_ids
            offset += batch_size
            await asyncio.sleep(1)
            gc.collect()

    async def _get_ordinals_ids(self, async_session, batch_size=10000, offset=None, cutoff_date=datetime.now() - timedelta(days=32)):
        try:
            async with async_session() as session:
                results = await session.execute(
                    select(OrdinalModel).filter(OrdinalModel.timestamp < int(datetime.timestamp(cutoff_date))).offset(offset).limit(batch_size))
                results = results.scalars().all()
            return results
        except Exception as e:
            logging.getLogger("db_optimization").error(
                f"Message: Something happened during ordinals request\n"
                f"Module: assets/src/database/crud.py\n"
                f"Type: {e}\n"
                f"Details: {traceback.format_exc()}")

    async def _migrate_ordinal_ids(self, batch_results, processed_ids, async_session, batch_processor=None):
        # Batch processing
        for ordinal in batch_results:
            ordinal_dict = {key: value for key, value in ordinal.__dict__.items() if key != "_sa_instance_state"}
            ordinal_dict["blocks"] = []
            try:
                ordinal = OrdinalSchema(**ordinal_dict)
                await batch_processor.add_to_batch(OldOrdinalModel(**ordinal.__dict__), async_session)
            except IntegrityError as e:
                logging.getLogger("db_optimization").error(
                    f"Message: Ordinal index {ordinal_dict["id"]} already present in the old_ordinals table\n"     
                    f"Module: assets/src/database/crud.py\n"
                    f"Type: {e}\n"
                    f"Details: {traceback.format_exc()}")
            except pydantic.ValidationError as e:
                logging.getLogger("db_optimization").error(
                    f"Message: Validation for index {ordinal_dict["id"]} failed\n"
                    f"Module: assets/src/database/crud.py\n"
                    f"Type: {e}\n"
                    f"Details: {traceback.format_exc()}")
            finally:
                processed_ids.add(ordinal_dict["id"])
        return processed_ids

    async def _delete_ordinal_ids(self, processed_ids: set, async_session: async_sessionmaker[AsyncSession]):
        # Deleting old data after processing current batch
        try:
            async with async_session() as session:
                if processed_ids:
                    await session.execute(
                        delete(OrdinalModel).filter(OrdinalModel.id.in_(processed_ids))
                    )
                    await session.commit()
        except Exception as e:
            logging.getLogger("db_optimization").error(f"Message: Something happened during node data deletion\n"
                                                       f"Module: assets/src/database/crud.py\n"
                                                       f"Type: {e}\n"
                                                       f"Details: {traceback.format_exc()}")

    async def migrate_old_ordinals(self, async_session: async_sessionmaker[AsyncSession], configuration):
        logging.getLogger("db_optimization").info(f"Ordinal data migration initiated.")
        batch_size = 10000
        batch_processor = DatabaseBatchProcessor(batch_size)
        offset = 0

        # Query for old data
        while True:
            processed_ids = set()
            batch_results = await self._get_ordinals_ids(async_session, batch_size=batch_size, offset=offset, cutoff_date=datetime.now() - timedelta(days=int(configuration["general"]["save data (days)"])))
            if not batch_results:
                logging.getLogger("db_optimization").info(f"No more ordinals to migrate")
                break  # No more data

            processed_ids = await self._migrate_ordinal_ids(batch_results, processed_ids, async_session, batch_processor=batch_processor)
            await self._delete_ordinal_ids(processed_ids, async_session)

            del batch_results
            del processed_ids
            offset += batch_size
            await asyncio.sleep(1)
            gc.collect()

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

    def _calculate_html_stats_yield(self, reward_results):
        principal = 250000

        # MPY Calculation
        monthly_interest_rate = reward_results.dag_address_sum / principal
        calculated_mpy_percentage = monthly_interest_rate * 100

        # APY Calculation
        estimated_apy = (1 + monthly_interest_rate) ** 12 - 1

        # Convert APY to percentage
        estimated_apy_percentage = estimated_apy * 100
        return calculated_mpy_percentage, estimated_apy_percentage

    async def _get_html_stats_rewards_and_metrics(self, dag_address, session):
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

        return reward_results.scalar_one_or_none(), metric_results.fetchall()

    async def _get_html_stats_price_values(self):
        from assets.src.database.database import get_latest_db_price
        price_timestamp, price_dagusd = await get_latest_db_price()
        price_dagusd = 0 if price_dagusd is None else price_dagusd
        price_timestamp = "ERROR!" if price_timestamp is None else price_timestamp
        return price_timestamp, price_dagusd

    async def get_html_page_stats(
            self,
            request,
            templates,
            dag_address,
            async_session: async_sessionmaker[AsyncSession],
    ):

        async with async_session() as session:
            reward_results, metric_results = await self._get_html_stats_rewards_and_metrics(dag_address, session)

            metric_dicts = []
            for node_metrics in metric_results:
                metric_dicts.append(node_metrics[0].__dict__)
            metric_dicts = sorted(metric_dicts, key=lambda d: d["layer"])

            price_timestamp, price_dagusd = await self._get_html_stats_price_values()

            if price_dagusd != 0.000000000:
                dag_earnings_price_now = reward_results.dag_address_sum * price_dagusd
            # MPY and APY
            calculated_mpy_percentage, estimated_apy_percentage = self._calculate_html_stats_yield(reward_results)

            # What those addresses earning more is earning (standard deviation)

            dag_earnings_price_now_dev = float(dag_earnings_price_now - reward_results.usd_address_sum)
            if dag_earnings_price_now_dev > 0:
                dag_earnings_price_now_dev = f'+{round(dag_earnings_price_now_dev, 2)}'
            else:
                dag_earnings_price_now_dev = f'{round(dag_earnings_price_now_dev, 2)}'

            content = templates.TemplateResponse(
                "stats.html",
                dict(request=request,
                     dag_address=reward_results.destinations,
                     percent_earning_more=round(reward_results.percent_earning_more, 2),
                     dag_address_sum=round(reward_results.dag_address_sum, 2),
                     dag_median_sum=round(reward_results.dag_median_sum, 2),
                     dag_address_daily_mean=round(reward_results.dag_address_daily_mean, 2),
                     dag_address_daily_std_dev=round(reward_results.dag_address_daily_sum_dev, 2),
                     dag_price_now=round(price_dagusd, 4),
                     dag_earnings_price_now_dev=dag_earnings_price_now_dev,
                     dag_price_now_timestamp=datetime.fromtimestamp(price_timestamp),
                     dag_earnings_price_now=round(dag_earnings_price_now, 2),
                     usd_address_sum=round(reward_results.usd_address_sum, 2),
                     usd_address_daily_sum=round(reward_results.usd_address_daily_sum, 2),
                     rewards_plot_path=f"rewards_{dag_address}.html",
                     cpu_plot_path=f"cpu_{dag_address}.html",
                     # Sum of all $DAG minted, minus very high earning wallets (Stardust Collective wallet, etc.)
                     dag_minted_for_validators=round(reward_results.nonoutlier_dag_addresses_minted_sum, 2),
                     # Highest earning address, minus very high earning wallets (Stardust Collective wallet, etc.)
                     dag_highest_earner=round(reward_results.above_dag_address_earner_highest, 2),
                     # What the address is missing out on (average)
                     above_dag_address_deviation_from_mean=round(
                         reward_results.above_dag_address_earnings_deviation_from_mean, 2),
                     # What the address is missing out on (compared to the highest earning address)
                     above_dag_address_deviation_from_highest_earning=round(
                         reward_results.above_dag_address_earnings_from_highest, 2),
                     metric_dicts=metric_dicts,
                     calculated_mpy=round(calculated_mpy_percentage, 2),
                     estimated_apy=round(estimated_apy_percentage, 2)
                     )
            )
            if reward_results:
                return content

    async def get_html_page_index(self, request, templates):
        content = templates.TemplateResponse(
            "index.html", dict(request=request))
        return content

    async def get_html_page_statistics(self, request, templates, async_session: async_sessionmaker[AsyncSession]):
        try:
            user_data = []
            unique_subscribers_l0 = []
            unique_subscribers_l1 = []
            integrationnet_node_count_l0 = 0
            testnet_node_count_l0 = 0
            mainnet_node_count_l0 = 0
            integrationnet_node_count_l1 = 0
            testnet_node_count_l1 = 0
            mainnet_node_count_l1 = 0
            marked_removable_node_count_l0 = 0
            marked_removable_node_count_l1 = 0

            async with async_session() as session:

                # Query to fetch all rows
                result = await session.execute(select(UserModel))
                all_rows = result.scalars().all()
            for node in all_rows:
                node_dict = node.__dict__
                if node_dict["cluster"] == "integrationnet":
                    if node_dict["layer"] == 0:
                        integrationnet_node_count_l0 += 1
                    else:
                        integrationnet_node_count_l1 += 1
                elif node_dict["cluster"] == "mainnet":
                    if node_dict["layer"] == 0:
                        mainnet_node_count_l0 += 1
                    else:
                        mainnet_node_count_l1 += 1
                elif node_dict["cluster"] == "testnet":
                    if node_dict["layer"] == 0:
                        testnet_node_count_l0 += 1
                    else:
                        testnet_node_count_l1 += 1
                elif node_dict["removal_datetime"]:
                    if node_dict["layer"] == 0:
                        marked_removable_node_count_l0 += 1
                    else:
                        marked_removable_node_count_l1 += 1
                if node_dict["layer"] == 0:
                    unique_subscribers_l0.append(node_dict["name"])
                else:
                    unique_subscribers_l1.append(node_dict["name"])
                user_data.append(node_dict)

            active_subscribed_nodes_count = mainnet_node_count_l0 + mainnet_node_count_l1 + testnet_node_count_l0 + testnet_node_count_l1 + integrationnet_node_count_l0 + integrationnet_node_count_l1
            inactive_subscribed_nodes_count = marked_removable_node_count_l0 + marked_removable_node_count_l1
            unique_subscribers_l0_count = len(list(set(unique_subscribers_l0)))
            unique_subscribers_l1_count = len(list(set(unique_subscribers_l1)))
            unique_subscribers_count_total = len(list(set(unique_subscribers_l0 + unique_subscribers_l1)))

            mainnet_durations_l0 = []
            mainnet_durations_l1 = []
            integrationnet_durations_l0 = []
            integrationnet_durations_l1 = []
            testnet_durations_l0 = []
            testnet_durations_l1 = []

            mainnet_durations_l0 = np.average(mainnet_durations_l0) if mainnet_durations_l0 else "Forthcoming"
            mainnet_durations_l1 = np.average(mainnet_durations_l1) if mainnet_durations_l1 else "Forthcoming"
            integrationnet_durations_l0 = np.average(integrationnet_durations_l0) if integrationnet_durations_l0 else "Forthcoming"
            integrationnet_durations_l1 = np.average(integrationnet_durations_l1) if integrationnet_durations_l1 else "Forthcoming"
            testnet_durations_l0 = np.average(testnet_durations_l0) if testnet_durations_l0 else "Forthcoming"
            testnet_durations_l1 = np.average(testnet_durations_l1) if testnet_durations_l1 else "Forthcoming"
            content = templates.TemplateResponse(
                "pages/statistics.html", dict(request=request,
                                              node_count_sum=len(user_data),
                                              mainnet_node_count_l0=mainnet_node_count_l0,
                                              mainnet_node_count_l1=mainnet_node_count_l1,
                                              integrationnet_node_count_l0=integrationnet_node_count_l0,
                                              integrationnet_node_count_l1=integrationnet_node_count_l1,
                                              testnet_node_count_l0=testnet_node_count_l0,
                                              testnet_node_count_l1=testnet_node_count_l1,
                                              active_subscribed_nodes_count=active_subscribed_nodes_count,
                                              inactive_subscribed_nodes_count=inactive_subscribed_nodes_count,
                                              mainnet_durations_l0=mainnet_durations_l0,
                                              mainnet_durations_l1=mainnet_durations_l1,
                                              integrationnet_durations_l0=integrationnet_durations_l0,
                                              integrationnet_durations_l1=integrationnet_durations_l1,
                                              testnet_durations_l0=testnet_durations_l0,
                                              testnet_durations_l1=testnet_durations_l1,
                                              unique_subscribers_count_total=unique_subscribers_count_total))
            return content
        except Exception:
            logging.getLogger("others").error(traceback.format_exc())

    async def get_html_page_about(selfself, request, templates):
        content = templates.TemplateResponse(
            "pages/about.html", dict(request=request))
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
            batch_size = 10000
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
                            OrdinalModel
                        )
                        .filter(OrdinalModel.timestamp >= timestamp)
                        .offset(offset)
                        .limit(batch_size)
                    )
                    logging.getLogger("stats").debug(f"Get ordinals from timestamp: {timestamp}, offset: {offset}")
                    results = await session.execute(statement)
                    batch_results = results.scalars().all()
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

                del results
                del batch_results
                offset += batch_size
                await asyncio.sleep(1)
                gc.collect()

        return data

    async def get_historic_node_data_from_timestamp(
            self, timestamp: int, async_session: async_sessionmaker[AsyncSession]
    ):
        """
        Get timeslice data from the node database.
        """
        one_gigabyte = 1073741824
        async with async_session() as session:
            batch_size = 10000
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
                        NodeModel
                    )
                    .filter(NodeModel.timestamp_index >= timestamp_datetime)
                    .offset(offset)
                    .limit(batch_size)
                )
                logging.getLogger("stats").debug(f"Get node_data from timestamp: {timestamp}, offset: {offset}")
                try:
                    results = await session.execute(statement)
                except aiohttp.client_exceptions.ServerDisconnectedError:
                    await asyncio.sleep(3)
                    continue
                batch_results = results.scalars().all()

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

                del results
                del batch_results
                offset += batch_size
                await asyncio.sleep(1)
                gc.collect()

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
    ) -> List:
        """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT, REMOVAL_DATETIME, CLUSTER!!!! Returns a list of all user IDs currently subscribed"""
        list_of_tuples = []
        async with async_session() as session:
            statement = select(UserModel).where(UserModel.layer == layer)
            results = await session.execute(statement)
            ids = results.scalars().all()
            for values in ids:
                list_of_tuples.append(
                    (values.id, values.ip, values.public_port, values.removal_datetime, values.cluster))
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

class Subscriber:
    async def from_ip(self, ip: str, async_session: async_sessionmaker[AsyncSession]):
        """
        Get subscriber data from IP
        :param ip:
        :param async_session:
        :return:
        """
        async with async_session() as session:
            results = await session.execute(
                select(UserModel).where(UserModel.ip == ip)
            )
            return results.scalars().all()


