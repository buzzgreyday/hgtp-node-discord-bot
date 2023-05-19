from aiofiles import os
import logging
from datetime import datetime
import pandas as pd
import dask.dataframe as dd


async def history(configuration: dict):
    if not await os.path.exists(configuration["file settings"]["locations"]["history_new"]):
        logging.warning(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE DATA NOT FOUND, RETURN BLANK DATAFRAME WITH COLUMNS")
        df = pd.DataFrame(columns=configuration["file settings"]["columns"]["history_new"])
        return dd.from_pandas(df, npartitions=1)
    elif await os.path.exists(configuration["file settings"]["locations"]["history_new"]):
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE DATA FOUND, RETURN READ DATAFRAME")
        return dd.read_parquet(configuration["file settings"]["locations"]["history_new"], columns=configuration["file settings"]["columns"]["history_new"])


async def subscribers(configuration: dict):
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - READING SUBSCRIBER DATA AND RETURNING DATAFRAME")
    if not await os.path.exists(configuration["file settings"]["locations"]["subscribers_new"]):
        df = pd.DataFrame(columns=configuration["file settings"]["columns"]["subscribers_new"])
        return dd.from_pandas(df, npartitions=1)
    elif await os.path.exists(configuration["file settings"]["locations"]["subscribers_new"]):
        return dd.read_parquet(configuration["file settings"]["locations"]["subscribers_new"], columns=configuration["file settings"]["columns"]["subscribers_new"])
