import logging
from datetime import datetime

import pandas as pd
import dask.dataframe as dd


async def history(dask_client, node_data, configuration):
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


async def subscriber(dask_client, list_of_subs, configuration):
    subscriber = dd.from_pandas(pd.DataFrame(list_of_subs), npartitions=1)
    fut = subscriber.to_parquet(configuration["file settings"]["locations"]["subscribers_new"], append=True, overwrite=False, compute=False, write_index=False)
    await dask_client.compute(fut)

