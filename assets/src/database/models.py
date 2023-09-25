from typing import Optional

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import datetime

class SQLBase(DeclarativeBase):
    pass

class User(SQLBase):
    """SQL Base for user subscription data"""
    __tablename__ = "users"

    index: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    wallet: Mapped[str]
    id: Mapped[str]
    ip: Mapped[str]
    public_port: Mapped[int]
    layer: Mapped[int]
    contact: Mapped[str]
    date: Mapped[datetime.datetime]
    type: Mapped[str]


class NodeData(SQLBase):
    """SQL Base for automatic check node data"""

    __tablename__ = "data"

    index: Mapped[int] = mapped_column(primary_key=True)
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
    former_cluster_association_time: Mapped[Optional[float]] = mapped_column(nullable=True)
    former_cluster_connectivity: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_node_cluster_session: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_cluster_dissociation_time: Mapped[Optional[float]] = mapped_column(nullable=True)
    former_cluster_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_cluster_peer_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    former_cluster_state: Mapped[Optional[str]] = mapped_column(nullable=True)
    former_reward_state: Mapped[Optional[bool]] = mapped_column(nullable=True)
    former_timestamp_index: Mapped[Optional[datetime.datetime]] = mapped_column(nullable=True)
    ip: Mapped[Optional[str]] = mapped_column(nullable=True)
    id: Mapped[Optional[str]] = mapped_column(nullable=True)
    last_notified_timestamp: Mapped[Optional[datetime.datetime]] = mapped_column(nullable=True)
    latest_cluster_session: Mapped[Optional[str]] = mapped_column(nullable=True)
    latest_version: Mapped[Optional[str]] = mapped_column(nullable=True)
    layer: Mapped[Optional[int]] = mapped_column(nullable=True)
    name: Mapped[Optional[str]] = mapped_column(nullable=True)
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
    timestamp_index: Mapped[Optional[datetime.datetime]] = mapped_column(nullable=True)
    version: Mapped[Optional[str]] = mapped_column(nullable=True)
    cluster_check_ordinal: Mapped[Optional[str]] = mapped_column(nullable=True)