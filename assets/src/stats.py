import asyncio
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from aiohttp import ClientSession, TCPConnector
import matplotlib.pyplot as plt

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
        print("Got data")
        data = pd.DataFrame(data)
        print("Data loaded into DataFrame")
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
        # One hour in seconds
        secs = 86400
        while t >= data['timestamp'].values.min():
            daily_df = data[(data['timestamp'] >= t - secs) & (data['timestamp'] <= t)].copy()
            daily_df.loc[:, 'dag_address_sum'] = daily_df.groupby('destinations')['dag'].transform('sum')
            print("transform was okay")
            daily_df.loc[:, 'dag_address_mean'] = daily_df.groupby('destinations')['dag_address_sum'].transform('mean')

            daily_df = daily_df[['timestamp', 'destinations', 'dag_address_sum', 'dag_address_mean']].drop_duplicates('destinations', ignore_index=True)
            list_of_df.append(daily_df)
            t = t - secs
        daily_df = pd.concat(list_of_df, ignore_index=True)
        overall_daily_median = daily_df['dag_address_mean'].median()

        # TEST PLOT CREATION:
        unique_destinations = daily_df['destinations'].unique()

        for destination in unique_destinations:
            destination_df = daily_df[daily_df['destinations'] == destination]

            plt.figure(figsize=(12, 6))
            plt.plot(destination_df.index + 1, destination_df['dag_address_mean'], marker='o')
            plt.axhline(overall_daily_median, color='red', linestyle='--', label='Overall Daily Median')

            plt.xlabel('')
            plt.ylabel('Daily $DAG Earnings')
            plt.title('')
            plt.xticks(rotation=45)  # Rotate x-axis labels for better readability if needed
            plt.grid(True)
            plt.tight_layout()
            plt.show()

        ###

        daily_df['dag_daily_std_dev'] = daily_df.groupby('destinations')['dag_address_sum'].transform('std')
        daily_df['dag_address_daily_mean'] = daily_df.groupby('destinations')['dag_address_sum'].transform('mean')
        daily_overall_median = daily_df['dag_address_sum'].median()

        # PERFORMING BETTER THAN THE REST ON THE DAILY, IF HIGHER
        daily_df['dag_address_daily_sum_dev'] = daily_df['dag_address_sum'] - daily_overall_median
        daily_df = daily_df[['destinations', 'dag_address_daily_mean', 'dag_address_daily_sum_dev', 'dag_daily_std_dev']].drop_duplicates('destinations')
        daily_df['dag_daily_std_dev'].fillna(0, inplace=True)
        print("Fill na done")
        daily_df = daily_df.sort_values(by=['dag_address_daily_sum_dev', 'dag_address_daily_mean', 'dag_daily_std_dev'], ascending=[False, False, True]).reset_index(drop=True)
        print("Sorting done")
        daily_df['daily_effective_score'] = daily_df.index + 1
        print(daily_df)

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

        data = data[['daily_effective_score', 'destinations', 'dag_address_sum', 'dag_address_sum_dev', 'dag_address_daily_sum_dev', 'dag_address_daily_mean', 'dag_daily_std_dev']].drop_duplicates('destinations')
        # MOST EFFECTIVE EARNER MUST BE THE LOWEST DAILY STD DEV AND THE HIGHEST TIMESPAN DEV EARNINGS
        data = data.sort_values(
            by=['dag_address_sum_dev', 'dag_address_daily_sum_dev', 'dag_address_daily_mean', 'dag_daily_std_dev'],
            ascending=[False, False, False, True]).reset_index(
            drop=True)
        # THE SCORE BELOW SHOULD PROBABLY RELY ON AN AGGREGATE SCORE OF DAG ADDRESS DAILY SUM DEV + DAG ADDRESS SUM DEV + DAG ADDRESS DAILY MEAN BEING HIGH
        # AND DAG DAILY STD DEV BEING LOW
        data['overall_effective_score'] = data.index + 1
        print(data)
