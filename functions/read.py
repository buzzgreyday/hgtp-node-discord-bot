import os.path
import logging
from datetime import datetime
import pandas as pd
import dask.dataframe as dd

async def history(configuration: dict):
    if not os.path.exists(configuration["file settings"]["locations"]["history"]):
        logging.warning(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE DATA NOT FOUND, RETURN BLANK DATAFRAME WITH COLUMNS")
        df = pd.DataFrame(columns=configuration["file settings"]["columns"]["history"])
        return dd.from_pandas(df, npartitions=1)
    elif os.path.exists(configuration["file settings"]["locations"]["history"]):
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE DATA FOUND, RETURN READ DATAFRAME")
        return dd.read_parquet(configuration["file settings"]["locations"]["history"], columns=configuration["file settings"]["columns"]["history"])


async def subscribers(configuration: dict):
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - READING SUBSCRIBER DATA AND RETURNING DATAFRAME")
    if not os.path.exists(configuration["file settings"]["locations"]["subscribers"]):
        df = pd.DataFrame(columns=configuration["file settings"]["columns"]["subscribers"])
        return dd.from_pandas(df, npartitions=1)
    elif os.path.exists(configuration["file settings"]["locations"]["subscribers"]):
        return dd.read_csv(f'{configuration["file settings"]["locations"]["subscribers"]}', dtype=configuration["file settings"]["dtypes"]["subscribers"])
