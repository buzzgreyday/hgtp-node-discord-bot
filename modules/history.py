import logging
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
from aiofiles import os


async def node_data(dask_client, node_data: dict, history_dataframe):

    # history_node_dataframe = await dask_client.compute(history_dataframe[(history_dataframe["host"] == node_data["host"]) & (history_dataframe["publicPort"] == node_data["publicPort"])])
    if node_data["publicPort"] is not None:
        return await dask_client.compute(history_dataframe[(history_dataframe["host"] == node_data["host"]) & (history_dataframe["publicPort"] == float(node_data["publicPort"]))])
    else:
        return await dask_client.compute(history_dataframe[(history_dataframe["host"] == node_data["host"]) & (history_dataframe["publicPort"] == node_data["publicPort"])])


def former_node_data(historic_node_dataframe):
    return historic_node_dataframe[
        historic_node_dataframe["timestampIndex"] == historic_node_dataframe["timestampIndex"].max()]


async def write(dask_client, node_data, configuration):
    history_dataframe = dd.from_pandas(pd.DataFrame(node_data), npartitions=1)
    history_dataframe["publicPort"] = history_dataframe["publicPort"].astype(float)
    history_dataframe["clusterAssociationTime"] = history_dataframe["clusterAssociationTime"].astype(float)
    history_dataframe["clusterDissociationTime"] = history_dataframe["clusterDissociationTime"].astype(float)
    history_dataframe["formerTimestampIndex"] = history_dataframe["formerTimestampIndex"].astype(str)
    history_dataframe["rewardTrueCount"] = history_dataframe["rewardTrueCount"].astype(float)
    history_dataframe["rewardFalseCount"] = history_dataframe["rewardFalseCount"].astype(float)

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


def merge(node_data: dict, historic_node_dataframe) -> dict:
    # Might need refactoring when metagraphs is coming
    if not historic_node_dataframe.empty:
        node_data["formerClusterNames"] = Clean(historic_node_dataframe["clusterNames"]).make_lower()
        node_data["formerClusterConnectivity"] = Clean(historic_node_dataframe["clusterConnectivity"][historic_node_dataframe["clusterNames"] == node_data["formerClusterNames"]]).make_none()
        node_data["formerClusterAssociationTime"] = Clean(historic_node_dataframe["clusterAssociationTime"][historic_node_dataframe["clusterNames"] == node_data["formerClusterNames"]]).make_none()
        node_data["formerClusterDissociationTime"] = Clean(historic_node_dataframe["clusterDissociationTime"][historic_node_dataframe["clusterNames"] == node_data["formerClusterNames"]]).make_none()
        node_data["formerTimestampIndex"] = Clean(historic_node_dataframe["timestampIndex"]).make_none()
        node_data["lastNotifiedTimestamp"] = Clean(historic_node_dataframe["lastNotifiedTimestamp"]).make_none()
        if node_data["state"] == "Offline":
            node_data["id"] = Clean(historic_node_dataframe["id"]).make_none()
            node_data["nodeWalletAddress"] = Clean(historic_node_dataframe["nodeWalletAddress"]).make_none()
            node_data["version"] = Clean(historic_node_dataframe["version"]).make_none()
            node_data["cpuCount"] = float(historic_node_dataframe["cpuCount"])
            node_data["diskSpaceTotal"] = float(historic_node_dataframe["diskSpaceTotal"])
            node_data["diskSpaceFree"] = float(historic_node_dataframe["diskSpaceFree"])
    return node_data
