import datetime
import hashlib
import logging
import re
import traceback
from typing import List, Optional
import datetime as dt

import aiohttp
from pydantic import BaseModel, ValidationError, validator

from assets.src import api, subscription
from assets.src.encode_decode import id_to_dag_address

IP_REGEX = r'^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$'
EMAIL_REGEX = re.compile(r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+')


class NodeBase(BaseModel):
    """This class is the base from which any node related object inherits, it's also the base for user data"""

    name: str
    alias: Optional[str] = None
    id: str = None
    ip: str
    public_port: int
    layer: int


class NodeMetrics(BaseModel):
    """These node metrics can be set by accessing the "from_text(text)" classmethod using main- or testnet text resp"""

    cluster_association_time: Optional[float] = None
    cpu_count: Optional[int] = None
    one_m_system_load_average: Optional[float] = None
    disk_space_free: Optional[float] = None
    disk_space_total: Optional[float] = None

    @classmethod
    def from_txt(cls, text):
        """Only mainnet or testnet is currently supported"""

        lst = [
            "process_uptime_seconds{application=",
            "system_cpu_count{application=",
            "system_load_average_1m{application=",
            "disk_free_bytes{application=",
            "disk_total_bytes{application=",
        ]
        for line in text.split("\n"):
            for idx, item in enumerate(lst):
                if type(item) != str:
                    continue
                elif line.startswith(item):
                    lst[idx] = float(line.split(" ")[1])
        return cls(
            cluster_association_time=int(lst[0]),
            cpu_count=int(lst[1]),
            one_m_system_load_average=lst[2],
            disk_space_free=int(lst[3]),
            disk_space_total=int(lst[4]),
        )


class Node(NodeBase, NodeMetrics):
    """The base model for every user node check"""

    index: Optional[int] = None
    alias: Optional[str] = None
    discord: Optional[str] = None
    mail: Optional[str] = None
    phone: Optional[str] = None
    p2p_port: Optional[int] = None
    wallet_address: Optional[str] = None
    wallet_balance: Optional[float] = None
    cluster_name: Optional[str] = None
    former_cluster_name: Optional[str] = None
    last_known_cluster_name: Optional[str] = None
    state: Optional[str] = None
    cluster_state: Optional[str] = None
    former_state: Optional[str] = None
    cluster_connectivity: Optional[str] = None
    former_cluster_connectivity: Optional[str] = None
    former_node_cluster_session: Optional[str] = None
    reward_state: Optional[bool] = None
    former_reward_state: Optional[bool] = None
    reward_true_count: Optional[int] = None
    reward_false_count: Optional[int] = None
    former_cluster_association_time: Optional[float] = None
    cluster_dissociation_time: Optional[float] = None
    former_cluster_dissociation_time: Optional[float] = None
    node_cluster_session: Optional[str] = None
    latest_cluster_session: Optional[str] = None
    node_peer_count: Optional[int] = None
    cluster_peer_count: Optional[int] = None
    former_cluster_peer_count: Optional[int] = None
    version: Optional[str] = None
    cluster_version: Optional[str] = None
    latest_version: Optional[str] = None
    notify: Optional[bool] = None
    last_notified_timestamp: Optional[dt.datetime] = None
    last_notified_reason: Optional[str] = None
    timestamp_index: Optional[dt.datetime] = None
    former_timestamp_index: Optional[dt.datetime] = None
    cluster_check_ordinal: Optional[str] = None


class Cluster(BaseModel):
    """Will need to be tied to a wallet! The base model for every cluster data, this object is created pre-user node checks"""

    name: str
    id: str = None
    ip: str = None
    public_port: int = None
    layer: int
    wallet: str = None
    state: str = "offline"
    peer_count: int = 0
    session: str = None
    version: str = None
    latest_ordinal: int = 1
    latest_ordinal_timestamp: dt.datetime = None
    recently_rewarded: List = []
    peer_data: List = []


class User(NodeBase):
    """This class can create a user object which can be subscribed using different methods and transformations"""

    date: dt.datetime = dt.datetime.now(datetime.timezone.utc)
    index: Optional[int]
    discord: Optional[str | int | None] = None
    discord_dm_allowed: bool = True
    mail: str
    phone: Optional[str | None] = None
    wallet: str
    alias: Optional[str | None] = None
    removal_datetime: Optional[dt.datetime | None] = None
    cluster: Optional[str | None] = None
    customer_id: Optional[str | None] = None
    subscription_id: Optional[str | None] = None
    subscription_created: Optional[dt.datetime | None] = None
    current_subscription_period_start: Optional[dt.datetime | None] = None
    current_subscription_period_end: Optional[dt.datetime | None] = None

    # VALIDATE ID VALUE, CREATE CUSTOM EXCEPTION!

    @staticmethod
    async def get_id(session, ip: str, port: str, mode, configuration):
        """Will need refactoring before metagraph release. Some other way to validate node?"""
        if mode == "subscribe":
            print(f"Requesting: {ip}:{port}")
            try:
                node_data, status_code = await api.safe_request(
                    session, f"http://{ip}:{port}/node/info", configuration
                )
            except Exception as e:
                print(e)
                return
            else:
                return str(node_data["id"]) if node_data is not None else None
        else:
            return None


class RewardSchema(BaseModel):
    amount: int
    destination: str | None


class OrdinalSchema(BaseModel):
    """This class is the schema to validate the rewards table data"""

    timestamp: int
    destination: str
    amount: float
    usd: Optional[float] = 0.0
    ordinal: int
    lastSnapshotHash: str | None
    height: int
    subHeight: int
    hash: str
    blocks: List[str | None]

    @validator("amount", pre=True)
    def amount_validate(cls, amount):
        return amount / 100000000


class PriceSchema(BaseModel):
    timestamp: int
    coin: Optional[str] = "DAG"
    usd: float


class RewardStatsSchema(BaseModel):
    destinations: str
    earner_score: int
    percent_earning_more: float
    dag_address_sum: float
    dag_median_sum: float
    daily_overall_median: float
    dag_address_sum_dev: float
    dag_address_daily_sum_dev: float
    dag_address_daily_sum: float
    dag_address_daily_mean: float
    dag_daily_std_dev: float
    count: int
    usd_address_sum: float
    usd_address_daily_sum: float
    dag_address_sum_zscore: float
    nonoutlier_dag_addresses_minted_sum: float
    above_dag_address_earner_highest: float
    above_dag_addresses_earnings_mean: float
    above_dag_address_earnings_deviation_from_mean: float
    above_dag_address_earnings_from_highest: float
    above_dag_address_earnings_std_dev: float


class MetricStatsSchema(BaseModel):
    hash_index: str
    destinations: str
    timestamp: int
    ip: str
    id: str
    public_port: int
    layer: int
    disk_free: float
    disk_total: float
    cpu_count: int
    daily_cpu_load: float


class StripeEvent(BaseModel):
    id: str
    type: str
    data: dict
