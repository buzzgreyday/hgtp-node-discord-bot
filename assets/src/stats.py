import asyncio
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from aiohttp import ClientSession, TCPConnector

from assets.src.rewards import normalize_timestamp, RequestSnapshot


async def run(configuration):
    await asyncio.sleep(10)
    async with ClientSession(connector=TCPConnector(
            # You need to obtain a real (non-self-signed certificate) to run in production
            # ssl=db.ssl_context.load_cert_chain(certfile=ssl_cert_file, keyfile=ssl_key_file)
            # Not intended for production:
            ssl=False)) as session:
        # Convert to Epoch
        # now = normalize_timestamp(datetime.utcnow().timestamp())
        timestamp = 1640995200
        # 30 days in seconds
        # seconds = 2592000
        data = await RequestSnapshot(session).database(f"http://127.0.0.1:8000/ordinal/from/{timestamp}")
        data = pd.DataFrame(data)
        # WE SHOULD NOW CREATE A DATABASE TABLE CONTAINING OVERALL HISTORIC PERFORMANCE OF ALL NODES UNTIL LATEST ORDINAL
        # TO GET EFFECTIVE EARNINGS, TAKE THE DAILY EARNINGS AND CALC THE STANDARD DEVIATION PER ADDRESS
        # PLUS DAILY MEAN
        # MOST EFFECTIVE EARNER MUST BE THE LOWEST DAILY STD DEV AND THE HIGHEST TIMESPAN DEV EARNINGS

        # ALSO NEED HIGHEST AND LOWEST EXPECTED DAILY EARNINGS AND

        # REMEMBER YOU TURNED OF THINGS IN MAIN.PY
        pd.set_option('display.max_rows', None)
        pd.options.display.float_format = '{:.2f}'.format
        t = data['timestamp'].values.max()
        list_of_df = []
        while t >= data['timestamp'].values.min():
            daily_df = data[(data['timestamp'] >= t - 86400) & (data['timestamp'] <= t)].copy()
            daily_df.loc[:, 'dag_address_sum'] = daily_df.groupby('destinations')['dag'].transform('sum')
            daily_df = daily_df[['destinations', 'dag_address_sum']].drop_duplicates('destinations', ignore_index=True)
            list_of_df.append(daily_df)
            t = t - 86400
        daily_df = pd.concat(list_of_df, ignore_index=True)
        daily_df['dag_daily_std_dev'] = daily_df.groupby('destinations')['dag_address_sum'].transform('std')
        daily_df['dag_address_daily_mean'] = daily_df.groupby('destinations')['dag_address_sum'].transform('mean')
        daily_overall_median = daily_df['dag_address_sum'].median()

        # PERFORMING BETTER THAN THE REST ON THE DAILY, IF HIGHER
        daily_df['dag_address_daily_sum_dev'] = daily_df['dag_address_sum'] - daily_overall_median
        daily_df = daily_df[['destinations', 'dag_address_daily_mean', 'dag_address_daily_sum_dev', 'dag_daily_std_dev']].drop_duplicates('destinations')
        daily_df['dag_daily_std_dev'].fillna(0, inplace=True)
        print(daily_df.sort_values(by=['dag_address_daily_sum_dev', 'dag_daily_std_dev'], ascending=False))

        input(f'Looks okay? ')
        # data['dag_effective_daily_earnings'] =
        data['dag_address_sum'] = data.groupby('destinations')['dag'].transform('sum')

        # THE USD VALUE NEEDS TO BE MULTIPLIED SINCE IT'S THE VALUE PER DAG
        # data['usd_sum'] = data.groupby('destinations')['usd_value_then'].transform('sum')
        # data['dag_overall_sum_median'] = data['dag_address_sum'].median()

        # PERFORMING BETTER THAN THE REST IN THE TIMESPAN, IF HIGHER
        data['dag_address_sum_dev'] = data['dag_address_sum'] - data['dag_address_sum'].median()
        data = data.merge(daily_df, on='destinations', how='left')
        """data['latest_ordinal'] = data['ordinals'].max()
        data['latest_timestamp'] = data['timestamps'].max()"""

        """data['first_received_rewards'] = data.groupby('destinations')['timestamps'].transform('min')
        data['latest_received_rewards'] = data.groupby('destinations')['timestamps'].transform('max')"""

        data = data[['destinations', 'dag_address_sum', 'dag_address_sum_dev', 'dag_address_daily_sum_dev', 'dag_address_daily_mean', 'dag_daily_std_dev']].drop_duplicates('destinations')
        # MOST EFFECTIVE EARNER MUST BE THE LOWEST DAILY STD DEV AND THE HIGHEST TIMESPAN DEV EARNINGS
        print(data.sort_values(by=['dag_address_sum_dev', 'dag_daily_std_dev'], ascending=False).reset_index())











