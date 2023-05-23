import logging
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
from aiofiles import os


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
