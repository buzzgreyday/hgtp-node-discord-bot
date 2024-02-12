import asyncio
import traceback
from datetime import datetime

import aiohttp
import pandas as pd
import sqlalchemy
from aiohttp import ClientSession, TCPConnector
import matplotlib.pyplot as plt
from asyncpg.exceptions import UniqueViolationError

from assets.src.database.database import post_stats, update_stats
from assets.src.rewards import RequestSnapshot, normalize_timestamp
from assets.src.schemas import StatSchema


sliced_columns = ['timestamp', "ordinals", 'destinations', 'dag_address_daily_sum', 'dag_address_daily_sum_dev',
                  'dag_daily_std_dev', 'daily_overall_median', 'dag_address_daily_mean', 'usd_per_token']
final_sliced_columns = ['destinations', 'dag_address_daily_mean', 'dag_address_daily_sum', 'dag_address_daily_sum_dev',
                        'dag_daily_std_dev', 'usd_address_daily_sum']
final_columns = ['daily_effectivity_score', 'destinations', 'dag_address_sum', 'dag_address_sum_dev', 'dag_median_sum',
                 'dag_address_daily_sum_dev', 'dag_address_daily_mean', 'dag_address_daily_sum', 'dag_daily_std_dev',
                 'usd_address_sum', 'usd_address_daily_sum']

async def sum_usd(data: pd.DataFrame, column_name: str):
    # THE USD VALUE NEEDS TO BE MULTIPLIED SINCE IT'S THE VALUE PER DAG
    usd_sum = data.groupby('destinations')['usd_per_token'].transform('mean')
    print(usd_sum)
    data[column_name] = usd_sum * data['dag_address_daily_sum']
    print('usd ok')
    return data

async def create_timeslice_data(data: pd.DataFrame, start_time: int, travers_seconds: int):
    """
    TO: Start time is usually the latest available timestamp
    FROM: Traverse seconds is for example seven days, one day or 24 hours in seconds (the time you wish to traverse)
    """
    list_of_df = []
    while start_time >= data['timestamp'].values.min():
        # Also add 7 days and 24 hours
        sliced_df = data[(data['timestamp'] >= start_time - travers_seconds) & (data['timestamp'] <= start_time)].copy()
        sliced_df.loc[:, 'dag_address_daily_sum'] = sliced_df.groupby('destinations')['dag'].transform('sum')
        print("transform was okay")
        sliced_df.loc[:, 'dag_address_daily_mean'] = sliced_df.groupby('destinations')['dag_address_daily_sum'].transform('mean')
        sliced_df.loc[:, 'daily_overall_median'] = sliced_df['dag_address_daily_sum'].median()
        sliced_df['dag_address_daily_sum_dev'] = sliced_df['dag_address_daily_sum'] - sliced_df['daily_overall_median']
        print("deviation okay")
        sliced_df['dag_daily_std_dev'] = sliced_df.groupby('destinations')['dag_address_daily_sum'].transform('std')

        sliced_df = sliced_df[sliced_columns].drop_duplicates('destinations', ignore_index=True)
        list_of_df.append(sliced_df)
        start_time = start_time - travers_seconds

    sliced_df = pd.concat(list_of_df, ignore_index=True)
    del list_of_df
    return sliced_df

async def create_daily_data(data: pd.DataFrame, start_time, from_timestamp):
    # One day in seconds
    traverse_seconds = 86400
    sliced_df = await create_timeslice_data(data, start_time, traverse_seconds)
    print('Timeslice done')
    sliced_df['dag_daily_std_dev'].fillna(0, inplace=True)
    create_visualizations(sliced_df, from_timestamp)
    print('Visualizations done')
    sliced_df = await sum_usd(sliced_df, 'usd_address_daily_sum')
    sliced_df = sliced_df[final_sliced_columns].drop_duplicates('destinations')
    sliced_df = sliced_df.sort_values(by=['dag_address_daily_sum_dev', 'dag_address_daily_mean', 'dag_daily_std_dev'],
                                      ascending=[False, False, True]).reset_index(drop=True)
    sliced_df['daily_effectivity_score'] = sliced_df.index
    print(sliced_df)
    return sliced_df


def create_visualizations(df: pd.DataFrame, from_timestamp: int):
    # Something here is causing a Tkinter related async issue: probably related to .close() or the fact that this was a
    # async function. Look into this.
    unique_destinations = df['destinations'].unique()
    path = "static"
    print("Starting loop")
    for destination in unique_destinations:
        destination_df = df[df['destinations'] == destination]
        plt.style.use('Solarize_Light2')
        fig = plt.figure(figsize=(10, 5))
        try:
            print("style ok")
            plt.plot(pd.to_datetime(destination_df['timestamp'] * 1000, unit='ms'),
                     destination_df['dag_address_daily_mean'], marker='o',
                     label='Daily node earnings')
            print("Daily node earnings plot: OK!")
            plt.plot(pd.to_datetime(destination_df['timestamp'] * 1000, unit='ms'),
                     destination_df['daily_overall_median'], marker='o',
                     label='Daily average network earnings', linestyle=':', alpha=0.5)
            print("Daily average network earnings plot: OK!")
            # Don't limit average to any particular address
            plt.axhline(df['dag_address_daily_mean'].median(), color='green', linestyle='--',
                        label=f'Average network earnings (since {datetime.fromtimestamp(timestamp=from_timestamp).strftime("%d. %B %Y")})',
                        alpha=0.5)
            plt.axhline(destination_df['dag_address_daily_mean'].median(), color='blue', linestyle='--',
                        label=f'Average node earnings (since {datetime.fromtimestamp(timestamp=from_timestamp).strftime("%d. %B %Y")})',
                        alpha=0.5)
            plt.xlabel('Time')
            plt.ylabel('$DAG Earnings')
            plt.title('')
        except Exception:
            print(traceback.format_exc())

        plt.legend()
        plt.xticks(rotation=45)  # Rotate x-axis labels for better readability if needed
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f"{path}/{destination}.jpg")
        # plt.show()
        plt.close()


async def run(configuration):
    await asyncio.sleep(10)

    """
    GET DATA
    """
    async with ClientSession(connector=TCPConnector(
            # You need to obtain a real (non-self-signed certificate) to run in production
            # ssl=db.ssl_context.load_cert_chain(certfile=ssl_cert_file, keyfile=ssl_key_file)
            # Not intended for production:
            ssl=False)) as session:
        # Convert to Epoch
        # timestamp = normalize_timestamp(datetime.utcnow().timestamp())
        timestamp = 1640995200
        # 30 days in seconds = 2592000
        # 7 days in seconds = 604800
        while True:
            try:
                data = await RequestSnapshot(session).database(f"http://127.0.0.1:8000/ordinal/from/{timestamp}")
            except aiohttp.client_exceptions.ClientConnectorError:
                await asyncio.sleep(3)
            else:
                break
        print("Got data")
        data = pd.DataFrame(data)
        # ! REMEMBER YOU TURNED OF THINGS IN MAIN.PY !
        pd.set_option('display.max_rows', None)
        pd.options.display.float_format = '{:.2f}'.format
        start_time = data['timestamp'].values.max()

        """
        CREATE DAILY DATA
        TO: Start time is the latest available timestamp
        FROM: The timestamp is the timestamp from where you wish to retrieve data from
        """
        sliced_df = await create_daily_data(data, start_time, timestamp)
        """
        CREATE OVERALL DATA
        """
        data = data.merge(sliced_df, on='destinations', how='left')
        try:
            data['dag_address_sum'] = data.groupby('destinations')['dag_address_daily_sum'].transform('sum')
            # data = await sum_usd(data, 'usd_address_sum')
            usd_sum = data.groupby('destinations')['usd_per_token'].transform('mean')
        except Exception:
            print(traceback.format_exc())
        data['usd_address_sum'] = usd_sum * data['dag_address_sum']
        print('usd ok')
        # The node is earning more than the average if sum deviation is positive
        data['dag_address_sum_dev'] = data['dag_address_sum'] - data['dag_address_sum'].median()
        print(1)
        data['dag_median_sum'] = data['dag_address_sum'].median()
        print(2)
        print(4)
        data = data[final_columns].drop_duplicates('destinations')
        """
        # The most effective node is the node with the lowest daily standard deviation, the highest daily mean earnings,
        # the highest daily sum deviation (average node sum earnings, minus network earnings median) and the highest
        # overall timeslice sum deviation (average node sum earnings, minus network earnings median).
        """
        print(5)
        data = data.sort_values(
            by=['dag_address_sum_dev', 'dag_address_daily_sum_dev', 'dag_address_daily_mean', 'dag_daily_std_dev'],
            ascending=[False, False, False, True]).reset_index(
            drop=True)
        """
        # The effectivity score: close to zero is good.
        """
        try:
            data['effectivity_score'] = (data.index + data['daily_effectivity_score']) / 2
        except ZeroDivisionError:
            data['effectivity_score'] = 0.0
        print(6)
        data = data.sort_values(by='dag_address_sum_dev', ascending=False).reset_index(drop=True)
        print(7)
        data['earner_score'] = data.index
        print(8)
        total_len = len(data.index)
        print(9)
        data['count'] = total_len
        print(10)
        # Initiate the row
        data['percent_earning_more'] = 0.0
        print(11)
        for i, row in data.iterrows():
            print(1)
            percentage = ((i + 1) / total_len) * 100
            print(2)
            data.at[i, 'percent_earning_more'] = percentage
            print(3)
            row['percent_earning_more'] = percentage
            print(4)
            schema = StatSchema(**row.to_dict())
            print(5)
            try:
                # Post
                await post_stats(schema)
            except sqlalchemy.exc.IntegrityError:
                await update_stats(schema)

        print(data)
        del data
