import os.path
import logging
from datetime import datetime
import pandas as pd
import dask.dataframe as dd
from functions.data_structure import Columns, Dtypes
import info


async def history(file):
    if not os.path.exists(file):
        logging.warning(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE DATA NOT FOUND, RETURN BLANK DATAFRAME WITH COLUMNS")
        return pd.DataFrame(columns=Columns.historic_node_data)
    elif os.path.exists(file):
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - NODE DATA FOUND, RETURN READ DATAFRAME")
        return dd.read_parquet(file, columns=Columns.historic_node_data)
    else:
        logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - SOMETHING WENT WRONG WHILE ATTEMPTING TO FIND NODE DATA")
        return pd.DataFrame()


async def subscribers():
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - READING SUBSCRIBER DATA AND RETURNING DATAFRAME")
    return dd.read_csv(info.subscriber_data, dtype=Dtypes.subscribers)
