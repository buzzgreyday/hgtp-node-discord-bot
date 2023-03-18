import os.path
import logging
from datetime import datetime
import pandas as pd
import dask.dataframe as dd


async def history(configuration):
    if not os.path.exists(configuration["file locations"]["history"]):
        logging.warning(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE DATA NOT FOUND, RETURN BLANK DATAFRAME WITH COLUMNS")
        return pd.DataFrame(columns=configuration["file locations"]["columns"]["history"])
    elif os.path.exists(configuration["file locations"]["history"]):
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE DATA FOUND, RETURN READ DATAFRAME")
        return dd.read_parquet(configuration["file locations"]["history"], columns=configuration["file locations"]["columns"]["history"])
    else:
        logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - SOMETHING WENT WRONG WHILE ATTEMPTING TO FIND NODE DATA")
        return pd.DataFrame()


async def subscribers(configuration):
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - READING SUBSCRIBER DATA AND RETURNING DATAFRAME")
    return dd.read_csv(f'{configuration["file locations"]["subscribers"]}', dtype=configuration["file locations"]["dtypes"]["subscribers"])


async def load_balancers(configuration):
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - READING LOAD BALANCER DATA AND RETURNING DATAFRAME")
    return dd.read_csv(f'{configuration["file locations"]["load balancers"]}', dtype=configuration["file locations"]["dtypes"]["subscribers"])
