import os.path
import logging
from datetime import datetime
import pandas as pd
import dask.dataframe as dd
from functions.format import Columns, Dtypes
import configuration


async def history():
    if not os.path.exists(configuration.latest_node_data):
        logging.warning(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE DATA NOT FOUND, RETURN BLANK DATAFRAME WITH COLUMNS")
        return pd.DataFrame(columns=Columns.historic_node_data)
    elif os.path.exists(configuration.latest_node_data):
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE DATA FOUND, RETURN READ DATAFRAME")
        return dd.read_parquet(configuration.latest_node_data, columns=Columns.historic_node_data)
    else:
        logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - SOMETHING WENT WRONG WHILE ATTEMPTING TO FIND NODE DATA")
        return pd.DataFrame()


async def subscribers():
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - READING SUBSCRIBER DATA AND RETURNING DATAFRAME")
    return dd.read_csv(configuration.subscriber_data, dtype=Dtypes.subscribers)


async def load_balancers():
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - READING LOAD BALANCER DATA AND RETURNING DATAFRAME")
    return dd.read_csv(configuration.load_balancers_data, dtype=Dtypes.load_balancers)
