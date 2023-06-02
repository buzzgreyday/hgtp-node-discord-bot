from typing import List
import datetime as dt

from pydantic import BaseModel


class NodeBase(BaseModel):
    name: str
    id: str
    ip: str
    public_port: int
    layer: int
    contact: str | int | None


class NodeMetrics(BaseModel):

    cluster_association_time: float = None
    cpu_count: float = None
    one_m_system_load_average: float = None
    disk_space_free: float = None
    disk_space_total: float = None

    @classmethod
    def from_txt(cls, text):
        comprehension = ['process_uptime_seconds{application=', 'system_cpu_count{application=', 'system_load_average_1m{application=', 'disk_free_bytes{application=', 'disk_total_bytes{application=']
        values = list(float(line.split(" ")[1]) for line in text.split("\n") for item in comprehension if line.startswith(comprehension[comprehension.index(item)]))
        return cls(cluster_association_time=values[0], cpu_count=values[1], one_m_system_load_average=values[2], disk_space_free=values[3], disk_space_total=values[4])


class Node(NodeBase, NodeMetrics):
    p2p_port: int = None
    wallet_address: str = None
    wallet_balance: float = None
    cluster_name: str = None
    former_cluster_name: str = None
    state: str = None
    cluster_state: str = None
    former_cluster_state: str = None
    cluster_connectivity: str = None
    former_cluster_connectivity: str = None
    reward_state: str = None
    former_reward_state: str = None
    reward_true_count: float = None
    reward_false_count: float = None
    former_cluster_association_time: float = None
    cluster_dissociation_time: float = None
    former_cluster_dissociation_time: float = None
    node_cluster_session: int = None
    latest_cluster_session: int = None
    node_peer_count: float = None
    cluster_peer_count: float = None
    former_cluster_peer_count: float = None
    version: str = None
    cluster_version: str = None
    latest_version: str = None
    notify: bool = None
    last_notified_timestamp: dt.datetime = None
    timestamp_index: dt.datetime = None
    former_timestamp_index: dt.datetime = None


class Cluster(NodeBase):
    state: str = "offline"
    peer_count: int = 0
    session: int = None
    version: str = None
    latest_ordinal: int = 1
    latest_ordinal_timestamp: dt.datetime = None
    recently_rewarded: List = []
    peer_data: List = []


class UserCreate(NodeBase):
    date: dt.datetime



