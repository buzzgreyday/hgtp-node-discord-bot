import re
from typing import List, Tuple
import datetime as dt

import pydantic
from pydantic import BaseModel


IP_REGEX = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"


class NodeBase(BaseModel):

    name: str
    id: str = None
    ip: str
    public_port: int
    layer: int
    contact: str | int | None


class NodeMetrics(BaseModel):

    cluster_association_time: int = None
    cpu_count: int = None
    one_m_system_load_average: float = None
    disk_space_free: int = None
    disk_space_total: int = None

    @classmethod
    def from_txt(cls, text):
        comprehension = ['process_uptime_seconds{application=', 'system_cpu_count{application=', 'system_load_average_1m{application=', 'disk_free_bytes{application=', 'disk_total_bytes{application=']
        values = list(float(line.split(" ")[1]) for line in text.split("\n") for item in comprehension if line.startswith(item))
        return cls(cluster_association_time=int(values[0]), cpu_count=int(values[1]), one_m_system_load_average=values[2], disk_space_free=int(values[3]), disk_space_total=int(values[4]))


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

    @classmethod
    def discord(cls, name: str, contact: int, id_: str, *args) -> List:
        subs = []
        """First the arguments are sliced per ip"""
        print(args)
        ips = list(set(filter(lambda ip: re.match(IP_REGEX, ip), args)))
        ip_idx = list(set(map(lambda ip: args.index(ip), ips)))
        for idx in range(0, len(ip_idx)):
            arg = args[ip_idx[idx]:ip_idx[idx + 1]] if idx + 1 < len(ip_idx) else args[ip_idx[idx]:]
            print(arg)
            """Then we clean the arguments"""
            ip = None
            for i, val in enumerate(arg):
                if re.match(IP_REGEX, val):
                    ip = val
                elif val.lower() in ("z", "zero", "l0"):
                    for port in arg[i + 1:]:
                        if port.isdigit():
                            subs.append(cls(name=name, contact=contact, id=id_, ip=ip, public_port=port, layer=0, date=dt.datetime.utcnow()))
                        else:
                            break
                elif val.lower() in ("o", "one", "l1"):
                    for port in arg[i + 1:]:
                        if port.isdigit():
                            subs.append(cls(name=name, contact=contact, id=id_, ip=ip, public_port=port, layer=1, date=dt.datetime.utcnow()))
                        else:
                            break

        return subs








