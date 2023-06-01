import logging
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
from aiofiles import os

from assets.code import schemas


class Clean:
    def __init__(self, value):
        self._value = value

    def make_lower(self) -> str | None:
        self._value = Clean(self._value).make_none()
        if self._value is not None:
            return self._value.lower()

    def make_none(self) -> str | None:
        if len(self._value) != 0:
            return self._value.values[0]
        else:
            return None


async def node_data(dask_client, node_data: schemas.Node, history_dataframe):
    if node_data.public_port is not None:
        return await dask_client.compute(history_dataframe[(history_dataframe["ip"] == node_data.ip) & (history_dataframe["public_port"] == node_data.public_port)])
    else:
        return await dask_client.compute(history_dataframe[(history_dataframe["ip"] == node_data.ip) & (history_dataframe["public_port"] == node_data.public_port)])


def former_node_data(historic_node_dataframe):
    return historic_node_dataframe[
        historic_node_dataframe["timestamp_index"] == historic_node_dataframe["timestamp_index"].max()]


async def write(dask_client, node_data: schemas.Node, configuration):
    history_dataframe = dd.from_pandas(pd.DataFrame(node_data.dict()), npartitions=1)
    """history_dataframe["publicPort"] = history_dataframe["publicPort"].astype(float)
    history_dataframe["clusterAssociationTime"] = history_dataframe["clusterAssociationTime"].astype(float)
    history_dataframe["clusterAssociationTime"] = history_dataframe["clusterAssociationTime"].astype(float)
    history_dataframe["clusterDissociationTime"] = history_dataframe["clusterDissociationTime"].astype(float)
    history_dataframe["formerTimestampIndex"] = history_dataframe["formerTimestampIndex"].astype(str)
    history_dataframe["rewardTrueCount"] = history_dataframe["rewardTrueCount"].astype(float)
    history_dataframe["rewardFalseCount"] = history_dataframe["rewardFalseCount"].astype(float)
    history_dataframe["rewardState"] = history_dataframe["rewardState"].astype(bool)"""

    fut = history_dataframe.to_parquet(configuration["file settings"]["locations"]["history_new"], overwrite=False, compute=False, write_index=False)
    await dask_client.compute(fut)
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - Writing history to parquet")


async def read(configuration: dict):
    if not await os.path.exists(configuration["file settings"]["locations"]["history_new"]):
        logging.warning(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE DATA NOT FOUND, RETURN BLANK DATAFRAME WITH COLUMNS")
        df = pd.DataFrame(columns=configuration["file settings"]["columns"]["history_new"])
        return dd.from_pandas(df, npartitions=1)
    elif await os.path.exists(configuration["file settings"]["locations"]["history_new"]):
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE DATA FOUND, RETURN READ DATAFRAME")
        return dd.read_parquet(configuration["file settings"]["locations"]["history_new"], columns=configuration["file settings"]["columns"]["history_new"])


def merge_data(node_data: schemas.Node, cluster_data, historic_node_dataframe) -> schemas.Node:
    # Might need refactoring when metagraphs is coming
    if not historic_node_dataframe.empty:
        node_data.former_cluster_name = Clean(historic_node_dataframe["cluster_name"]).make_lower()
        node_data.former_cluster_connectivity = Clean(historic_node_dataframe["cluster_connectivity"][historic_node_dataframe["cluster_name"] == node_data.former_cluster_name]).make_none()
        node_data.former_cluster_association_time = Clean(historic_node_dataframe["cluster_association_time"][historic_node_dataframe["cluster_name"] == node_data.former_cluster_name]).make_none()
        node_data.former_cluster_dissociation_time = Clean(historic_node_dataframe["cluster_dissociation_time"][historic_node_dataframe["cluster_name"] == node_data.former_cluster_name]).make_none()
        node_data.former_timestamp_index = Clean(historic_node_dataframe["timestamp_index"]).make_none()
        node_data.last_notified_timestamp = Clean(historic_node_dataframe["last_notified_timestamp"]).make_none()
        if node_data.state == "Offline":
            node_data.id = Clean(historic_node_dataframe["id"]).make_none()
            node_data.wallet_address = Clean(historic_node_dataframe["wallet_address"]).make_none()
            node_data.version = Clean(historic_node_dataframe["version"]).make_none()
            node_data.cpu_count = float(historic_node_dataframe["cpu_count"])
            node_data.disk_space_total = float(historic_node_dataframe["disk_space_total"])
            node_data.disk_space_free = float(historic_node_dataframe["disk_space_free"])
        if cluster_data is not None:
            if cluster_data["cluster name"] == node_data.former_cluster_name:
                node_data.former_cluster_peer_count = cluster_data["peer count"]
                node_data.former_cluster_state = cluster_data["state"]

    return node_data
