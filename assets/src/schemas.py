import hashlib
import logging
import re
import traceback
from typing import List, Optional
import datetime as dt

import numpy as np
import pandas as pd
from pydantic import BaseModel, ValidationError, validator, field_validator

from assets.src import api
from assets.src.encode_decode import id_to_dag_address

IP_REGEX = r'^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$'


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

    date: dt.datetime = dt.datetime.utcnow()
    index: Optional[int]
    discord: Optional[str | int | None] = None
    mail: Optional[str | None] = None
    phone: Optional[str | None] = None
    wallet: str
    alias: Optional[str | None] = None

    # VALIDATE ID VALUE, CREATE CUSTOM EXCEPTION!

    @staticmethod
    async def get_id(session, ip: str, port: str, mode, configuration):
        """Will need refactoring before metagraph release. Some other way to validate node?"""
        if mode == "subscribe":
            node_data, status_code = await api.safe_request(
                session, f"http://{ip}:{port}/node/info", configuration
            )
            return str(node_data["id"]) if node_data is not None else None
        else:
            return None

    @classmethod
    async def sub_discord(
        cls,
        session,
        configuration,
        process_msg,
        mode: str,
        name: str,
        discord: int,
        *args,
    ):
        """Treats a Discord message as a line of arguments and returns a list of subscribable user objects"""
        from assets.src.discord.discord import update_subscription_process_msg

        user_data = []
        invalid_user_data = []
        wallet = None
        process_msg = await update_subscription_process_msg(process_msg, 1, None)
        ips = {ip for ip in args if re.match(IP_REGEX, ip)}
        ip_idx = [args.index(ip) for ip in ips]
        for idx in range(0, len(ip_idx)):
            # Split arguments into lists before each IP
            arg = (
                args[ip_idx[idx] : ip_idx[idx + 1]]
                if idx + 1 < len(ip_idx)
                else args[ip_idx[idx] :]
            )
            for i, val in enumerate(arg):
                if val.lower() in ("z", "-z", "zero", "--zero", "l0", "-l0"):
                    for port in arg[i + 1 :]:
                        if port.isdigit():
                            process_msg = await update_subscription_process_msg(
                                process_msg, 2, f"{arg[0]}:{port}"
                            )
                            # Check if port is subscribed?
                            id_ = await User.get_id(
                                session, arg[0], port, mode, configuration
                            )
                            if id_ is not None:
                                wallet = id_to_dag_address(id_)
                                try:
                                    user_data.append(
                                        cls(
                                            index=None,
                                            name=name,
                                            date=dt.datetime.utcnow(),
                                            discord=str(discord),
                                            id=id_,
                                            wallet=wallet,
                                            ip=arg[0],
                                            public_port=port,
                                            layer=0,
                                        )
                                    )
                                except ValidationError:
                                    logging.getLogger("app").warning(
                                        f"schemas.py - Pydantic ValidationError - subscription failed with the following traceback: {traceback.format_exc()}"
                                    )
                            else:
                                invalid_user_data.append((arg[0], port, 0))
                        else:
                            break
                elif val.lower() in ("o", "-o", "one", "--one", "l1", "-l1"):
                    for port in arg[i + 1 :]:
                        if port.isdigit():
                            process_msg = await update_subscription_process_msg(
                                process_msg, 2, f"{arg[0]}:{port}"
                            )
                            id_ = await User.get_id(
                                session, arg[0], port, mode, configuration
                            )
                            if id_ is not None:
                                wallet = id_to_dag_address(id_)
                                try:
                                    user_data.append(
                                        cls(
                                            index=None,
                                            name=name,
                                            date=dt.datetime.utcnow(),
                                            discord=str(discord),
                                            id=id_,
                                            wallet=wallet,
                                            ip=arg[0],
                                            public_port=port,
                                            layer=1,
                                        )
                                    )
                                except ValidationError:
                                    logging.getLogger("app").warning(
                                        f"schemas.py - Pydantic ValidationError - subscription failed with the following traceback: {traceback.format_exc()}"
                                    )
                            else:
                                invalid_user_data.append((arg[0], port, 1))
                        else:
                            break
        return user_data, invalid_user_data


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
    dag_address_sum_zscore: float
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

    # This is no longer needed as the hash is generated before
    """@validator("hash_index", pre=True)
    def auto_generate_hash(cls):
        key_str = f"{cls.id}-{cls.ip}-{cls.public_port}"
        hash_index = hashlib.sha256(key_str.encode()).hexdigest()
        print(f"{key_str}, {hash_index}")
        return hash_index"""
