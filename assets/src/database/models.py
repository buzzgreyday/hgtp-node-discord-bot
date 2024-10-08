from typing import Optional, List

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import datetime


class SQLBase(DeclarativeBase):
    pass


# Does not need "old data" table
class UserModel(SQLBase):
    """SQL Base for user subscription data"""

    __tablename__ = "users"

    index: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    wallet: Mapped[str]
    id: Mapped[str] = mapped_column(index=True)
    ip: Mapped[str]
    public_port: Mapped[int]
    cluster: Mapped[Optional[str]] = mapped_column(nullable=True)
    layer: Mapped[int]
    discord: Mapped[Optional[str]] = mapped_column(nullable=True)
    mail: Mapped[Optional[str]] = mapped_column(nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(nullable=True)
    date: Mapped[datetime.datetime]
    alias: Mapped[Optional[str]] = mapped_column(nullable=True)
    removal_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(nullable=True)


# Does need "old data" table
class NodeModel(SQLBase):
    """SQL Base for automatic check node data"""

    __tablename__ = "data"

    index: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    one_m_system_load_average: Mapped[Optional[float]] = mapped_column(nullable=True)
    cluster_association_time: Mapped[Optional[float]] = mapped_column(nullable=True)
    cluster_connectivity: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_dissociation_time: Mapped[Optional[float]] = mapped_column(nullable=True)
    cluster_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    last_known_cluster_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_peer_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    cluster_state: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_version: Mapped[Optional[str]] = mapped_column(nullable=True)
    contact: Mapped[Optional[str]] = mapped_column(nullable=True)
    cpu_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    disk_space_free: Mapped[Optional[float]] = mapped_column(nullable=True)
    disk_space_total: Mapped[Optional[float]] = mapped_column(nullable=True)
    former_cluster_association_time: Mapped[Optional[float]] = mapped_column(
        nullable=True
    )
    former_cluster_connectivity: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_node_cluster_session: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_cluster_dissociation_time: Mapped[Optional[float]] = mapped_column(
        nullable=True
    )
    former_cluster_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_cluster_peer_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    former_state: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_reward_state: Mapped[Optional[bool]] = mapped_column(nullable=True)
    former_timestamp_index: Mapped[Optional[datetime.datetime]] = mapped_column(
        nullable=True
    )
    ip: Mapped[Optional[str]] = mapped_column(nullable=True)
    id: Mapped[Optional[str]] = mapped_column(nullable=True, index=True)
    last_notified_timestamp: Mapped[Optional[datetime.datetime]] = mapped_column(
        nullable=True
    )
    last_notified_reason: Mapped[Optional[str]] = mapped_column(nullable=True)
    latest_cluster_session: Mapped[Optional[str]] = mapped_column(nullable=True)
    latest_version: Mapped[Optional[str]] = mapped_column(nullable=True)
    layer: Mapped[Optional[int]] = mapped_column(nullable=True)
    name: Mapped[Optional[str]] = mapped_column(nullable=True)
    alias: Mapped[Optional[str]] = mapped_column(nullable=True)
    discord: Mapped[Optional[str]] = mapped_column(nullable=True)
    mail: Mapped[Optional[str]] = mapped_column(nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(nullable=True)
    node_cluster_session: Mapped[Optional[str]] = mapped_column(nullable=True)
    node_peer_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    wallet_address: Mapped[Optional[str]] = mapped_column(nullable=True)
    wallet_balance: Mapped[Optional[float]] = mapped_column(nullable=True)
    notify: Mapped[Optional[bool]] = mapped_column(nullable=True)
    p2p_port: Mapped[Optional[int]] = mapped_column(nullable=True)
    public_port: Mapped[Optional[int]] = mapped_column(nullable=True)
    reward_false_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    reward_state: Mapped[Optional[bool]] = mapped_column(nullable=True)
    reward_true_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    state: Mapped[Optional[str]] = mapped_column(nullable=True)
    timestamp_index: Mapped[Optional[datetime.datetime]] = mapped_column(
        nullable=True, index=True
    )
    version: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_check_ordinal: Mapped[Optional[str]] = mapped_column(nullable=True)


class OldNodeModel(SQLBase):
    """SQL Base for automatic check node data (older than 30d)"""

    __tablename__ = "old_data"

    index: Mapped[int] = mapped_column(primary_key=True)  #, autoincrement=True)
    one_m_system_load_average: Mapped[Optional[float]] = mapped_column(nullable=True)
    cluster_association_time: Mapped[Optional[float]] = mapped_column(nullable=True)
    cluster_connectivity: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_dissociation_time: Mapped[Optional[float]] = mapped_column(nullable=True)
    cluster_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    last_known_cluster_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_peer_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    cluster_state: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_version: Mapped[Optional[str]] = mapped_column(nullable=True)
    contact: Mapped[Optional[str]] = mapped_column(nullable=True)
    cpu_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    disk_space_free: Mapped[Optional[float]] = mapped_column(nullable=True)
    disk_space_total: Mapped[Optional[float]] = mapped_column(nullable=True)
    former_cluster_association_time: Mapped[Optional[float]] = mapped_column(
        nullable=True
    )
    former_cluster_connectivity: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_node_cluster_session: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_cluster_dissociation_time: Mapped[Optional[float]] = mapped_column(
        nullable=True
    )
    former_cluster_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_cluster_peer_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    former_state: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_reward_state: Mapped[Optional[bool]] = mapped_column(nullable=True)
    former_timestamp_index: Mapped[Optional[datetime.datetime]] = mapped_column(
        nullable=True
    )
    ip: Mapped[Optional[str]] = mapped_column(nullable=True)
    id: Mapped[Optional[str]] = mapped_column(nullable=True, index=True)
    last_notified_timestamp: Mapped[Optional[datetime.datetime]] = mapped_column(
        nullable=True
    )
    last_notified_reason: Mapped[Optional[str]] = mapped_column(nullable=True)
    latest_cluster_session: Mapped[Optional[str]] = mapped_column(nullable=True)
    latest_version: Mapped[Optional[str]] = mapped_column(nullable=True)
    layer: Mapped[Optional[int]] = mapped_column(nullable=True)
    name: Mapped[Optional[str]] = mapped_column(nullable=True)
    alias: Mapped[Optional[str]] = mapped_column(nullable=True)
    discord: Mapped[Optional[str]] = mapped_column(nullable=True)
    mail: Mapped[Optional[str]] = mapped_column(nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(nullable=True)
    node_cluster_session: Mapped[Optional[str]] = mapped_column(nullable=True)
    node_peer_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    wallet_address: Mapped[Optional[str]] = mapped_column(nullable=True)
    wallet_balance: Mapped[Optional[float]] = mapped_column(nullable=True)
    notify: Mapped[Optional[bool]] = mapped_column(nullable=True)
    p2p_port: Mapped[Optional[int]] = mapped_column(nullable=True)
    public_port: Mapped[Optional[int]] = mapped_column(nullable=True)
    reward_false_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    reward_state: Mapped[Optional[bool]] = mapped_column(nullable=True)
    reward_true_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    state: Mapped[Optional[str]] = mapped_column(nullable=True)
    timestamp_index: Mapped[Optional[datetime.datetime]] = mapped_column(
        nullable=True, index=True
    )
    version: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_check_ordinal: Mapped[Optional[str]] = mapped_column(nullable=True)


# Does need "old data" model
class OrdinalModel(SQLBase):
    """SQL Base for reward and ordinal data"""

    __tablename__ = "ordinal"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    destination: Mapped[str] = mapped_column(index=True)
    amount: Mapped[float]
    usd: Mapped[float]
    hash: Mapped[str]
    ordinal: Mapped[int]
    height: Mapped[int]
    subHeight: Mapped[int]
    lastSnapshotHash: Mapped[str]
    blocks: Mapped[List[str | None]] = []
    timestamp: Mapped[int] = mapped_column(index=True)


class OldOrdinalModel(SQLBase):
    """SQL Base for old reward and ordinal data"""

    __tablename__ = "old_ordinal"

    id: Mapped[int] = mapped_column(primary_key=True)
    destination: Mapped[str] = mapped_column(index=True)
    amount: Mapped[float]
    usd: Mapped[float]
    hash: Mapped[str]
    ordinal: Mapped[int]
    height: Mapped[int]
    subHeight: Mapped[int]
    lastSnapshotHash: Mapped[str]
    blocks: Mapped[List[str | None]] = []
    timestamp: Mapped[int] = mapped_column(index=True)


# Does need old data model
class PriceModel(SQLBase):
    """The base for the Coingecko prices"""

    __tablename__ = "price"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    timestamp: Mapped[int] = mapped_column(index=True)
    coin: Mapped[str]
    usd: Mapped[float]


class OldPriceModel(PriceModel):
    """The base for the old Coingecko prices"""

    pass


# Does not need old data model
class RewardStatsModel(SQLBase):
    """SQL Base for statistical data"""

    __tablename__ = "reward_stats"

    destinations: Mapped[str] = mapped_column(index=True, primary_key=True)
    earner_score: Mapped[int]
    percent_earning_more: Mapped[float]
    dag_address_sum: Mapped[float]
    dag_address_sum_zscore: Mapped[float]
    dag_median_sum: Mapped[float]
    daily_overall_median: Mapped[float]
    dag_address_sum_dev: Mapped[float]
    dag_address_daily_sum_dev: Mapped[float]
    dag_address_daily_sum: Mapped[float]
    dag_address_daily_mean: Mapped[float]
    dag_daily_std_dev: Mapped[float]
    count: Mapped[int]
    usd_address_sum: Mapped[float]
    usd_address_daily_sum: Mapped[float]
    nonoutlier_dag_addresses_minted_sum: Mapped[float]
    above_dag_address_earner_highest: Mapped[float]
    above_dag_addresses_earnings_mean: Mapped[float]
    above_dag_address_earnings_deviation_from_mean: Mapped[float]
    above_dag_address_earnings_from_highest: Mapped[float]
    above_dag_address_earnings_std_dev: Mapped[float]


class MetricStatsModel(SQLBase):
    __tablename__ = "metric_stats"

    hash_index: Mapped[str] = mapped_column(primary_key=True)
    timestamp: Mapped[int]
    destinations: Mapped[str] = mapped_column(index=True)
    ip: Mapped[str] = mapped_column(index=True)
    id: Mapped[str]
    public_port: Mapped[int] = mapped_column(index=True)
    layer: Mapped[int]
    disk_free: Mapped[float]
    disk_total: Mapped[float]
    cpu_count: Mapped[int]
    daily_cpu_load: Mapped[float]
