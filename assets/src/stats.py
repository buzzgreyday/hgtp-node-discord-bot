import asyncio
import traceback
from datetime import datetime

import aiohttp
import pandas as pd
import sqlalchemy
from aiohttp import ClientSession, TCPConnector
import matplotlib.pyplot as plt

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


def sum_usd(df: pd.DataFrame, new_column_name: str, address_specific_sum_column) -> pd.DataFrame:
    # THE USD VALUE NEEDS TO BE MULTIPLIED SINCE IT'S THE VALUE PER DAG
    df[new_column_name] = df['usd_per_token'] * df[address_specific_sum_column]
    return df


def calculate_address_specific_sum(df: pd.DataFrame, new_column_name: str, address_specific_sum_column: str) -> pd.DataFrame:
    df.loc[:, new_column_name] = df.groupby('destinations')[address_specific_sum_column].transform('sum')
    return df


def calculate_address_specific_mean(df: pd.DataFrame, new_column_name: str, address_specific_mean_column: str)  -> pd.DataFrame:
    df.loc[:, new_column_name] = df.groupby('destinations')[address_specific_mean_column].transform(
        'mean')
    return df


def calculate_address_specific_deviation(sliced_df: pd.DataFrame, new_column_name: str, address_specific_sum_column, general_sum_column) -> pd.DataFrame:
    sliced_df[new_column_name] = sliced_df[address_specific_sum_column] - sliced_df[general_sum_column]
    return sliced_df


def calculate_address_specific_standard_deviation(df: pd.DataFrame, new_column_name: str, address_specific_sum_column: str) -> pd.DataFrame:
    df[new_column_name] = df.groupby('destinations')[address_specific_sum_column].transform('std')
    return df

def calculate_general_data_median(df: pd.DataFrame, new_column_name: str, median_column: str) -> pd.DataFrame:
    df.loc[:, new_column_name] = df[median_column].median()
    return df


def create_timeslice_data(data: pd.DataFrame, start_time: int, travers_seconds: int) -> pd.DataFrame:
    """
    TO: Start time is usually the latest available timestamp
    FROM: Traverse seconds is for example seven days, one day or 24 hours in seconds (the time you wish to traverse)
    """
    list_of_df = []
    while start_time >= data['timestamp'].values.min():
        # Also add 7 days and 24 hours
        sliced_df = data[(data['timestamp'] >= start_time - travers_seconds) & (data['timestamp'] <= start_time)].copy()
        sliced_df = calculate_address_specific_sum(sliced_df, 'dag_address_daily_sum', 'dag')
        # Clean the data
        list_of_df.append(sliced_df)
        print(f"Timeslice data transformation done, t >= {start_time}!")
        start_time = start_time - travers_seconds

    sliced_df = pd.concat(list_of_df, ignore_index=True)
    sliced_df = calculate_general_data_median(sliced_df, 'daily_overall_median', 'dag_address_daily_sum')
    sliced_df = calculate_address_specific_deviation(sliced_df, 'dag_address_daily_sum_dev', 'dag_address_daily_sum',
                                                     'daily_overall_median')
    sliced_df = calculate_address_specific_standard_deviation(sliced_df, 'dag_daily_std_dev', 'dag_address_daily_sum')
    sliced_df = calculate_address_specific_mean(sliced_df, 'dag_address_daily_mean', 'dag_address_daily_sum')
    sliced_df = sliced_df[sliced_columns].drop_duplicates('destinations', ignore_index=True)

    del list_of_df
    return sliced_df

def create_timeslice_effectivity_score(sliced_df: pd.DataFrame) -> pd.DataFrame:

    print("Scoring address specific daily effectivity...")
    sliced_df = sliced_df.sort_values(by=['dag_address_daily_sum_dev', 'dag_address_daily_mean', 'dag_daily_std_dev'],
                                      ascending=[False, False, True]).reset_index(drop=True)
    sliced_df['daily_effectivity_score'] = sliced_df.index
    print("Scoring done!")
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


async def get_data(session, timestamp):

    while True:
        try:
            snapshot_data = await RequestSnapshot(session).database(f"http://127.0.0.1:8000/ordinal/from/{timestamp}")
        except aiohttp.client_exceptions.ClientConnectorError:
            await asyncio.sleep(3)
        else:
            break
    print("Got snapshot_data")
    data = pd.DataFrame(snapshot_data)
    return data


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

        # Raw data calculations: I need the aggregated dag_address_sum from the snapshot data column "dag"
        snapshot_data = await get_data(session, timestamp)
        # ! REMEMBER YOU TURNED OF THINGS IN MAIN.PY !
        pd.set_option('display.max_rows', None)
        pd.options.display.float_format = '{:.2f}'.format
        """
        CREATE DAILY DATA
        TO: Start time is the latest available timestamp
        FROM: The "timestamp" var is the timestamp from where you wish to retrieve data from
        """
        start_time = snapshot_data['timestamp'].values.max()
        # One day in seconds
        traverse_seconds = 86400
        try:
            sliced_df = create_timeslice_data(snapshot_data, start_time, traverse_seconds)
        except Exception:
            print(traceback.format_exc())
        sliced_df['dag_daily_std_dev'].fillna(0, inplace=True)
        create_visualizations(sliced_df, timestamp)
        print('Visualizations done')
        try:
            sliced_df = sum_usd(sliced_df, 'usd_address_daily_sum', 'dag_address_daily_sum')
        except Exception:
            print(traceback.format_exc())
        # Clean the data
        print("Cleaning daily sliced data...")
        sliced_df = sliced_df[final_sliced_columns].drop_duplicates('destinations')
        print("Cleaning done!")
        print(sliced_df)
        input("Sliced_df looks clean? ")
        try:
            sliced_df = create_timeslice_effectivity_score(sliced_df)
        except Exception:
            print(traceback.format_exc())


        """
        CREATE OVERALL DATA
        """
        snapshot_data['dag_address_sum'] = snapshot_data.groupby('destinations')['dag'].transform('sum')
        print(snapshot_data.head(20))
        input("DAG general sum looks okay? ")

        print("Merging daily daily sliced data...")

        try:
            snapshot_data = sliced_df.merge(snapshot_data.drop_duplicates('destinations'), on='destinations',
                                            how='left')
            print(snapshot_data)
            input("Merged data looks okay? ")
        except Exception:
            print(traceback.format_exc())
        print("Merging done!")


        # snapshot_data = snapshot_data.drop_duplicates('destinations')

        try:
            snapshot_data = sum_usd(snapshot_data, 'usd_address_sum', 'dag_address_sum')
        except Exception:
            print(traceback.format_exc())
        print(snapshot_data)
        input("USD general sum looks okay? ")

        # The node is earning more than the average if sum deviation is positive
        try:
            snapshot_data['dag_address_sum_dev'] = snapshot_data['dag_address_sum'] - snapshot_data['dag_address_sum'].median()
        except Exception:
            print(traceback.format_exc())
        print(snapshot_data)
        input("Deviation looks okay? ")
        snapshot_data['dag_median_sum'] = snapshot_data['dag_address_sum'].median()
        print(snapshot_data)
        input("Median looks okay? ")

        """
        # The most effective node is the node with the lowest daily standard deviation, the highest daily mean earnings,
        # the highest daily sum deviation (average node sum earnings, minus network earnings median) and the highest
        # overall timeslice sum deviation (average node sum earnings, minus network earnings median).
        """
        print("Preparing effectivity_score...")
        try:
            snapshot_data = snapshot_data.sort_values(
                by=['dag_address_sum_dev', 'dag_address_daily_sum_dev', 'dag_address_daily_mean', 'dag_daily_std_dev'],
                ascending=[False, False, False, True]).reset_index(
                drop=True)
        except Exception:
            print(traceback.format_exc())
        """
        # The effectivity score: close to zero is good.
        """
        print("Creating effectivity score...")
        try:
            snapshot_data['effectivity_score'] = (snapshot_data.index + snapshot_data['daily_effectivity_score']) / 2
        except ZeroDivisionError:
            snapshot_data['effectivity_score'] = 0.0
        print("Score created!")
        try:
            snapshot_data = snapshot_data.sort_values(by='dag_address_sum_dev', ascending=False).reset_index(drop=True)
            snapshot_data['earner_score'] = snapshot_data.index
            total_len = len(snapshot_data.index)
            snapshot_data['count'] = total_len
            # Initiate the row
            snapshot_data['percent_earning_more'] = 0.0
        except Exception:
            print(traceback.format_exc())
        print("Post/update database statistics")
        try:
            for i, row in snapshot_data.iterrows():
                percentage = ((i + 1) / total_len) * 100
                snapshot_data.at[i, 'percent_earning_more'] = percentage
                row['percent_earning_more'] = percentage
                print(row)
                schema = StatSchema(**row.to_dict())
                try:
                    # Post
                    await post_stats(schema)
                except sqlalchemy.exc.IntegrityError:
                    await update_stats(schema)
        except Exception:
            print(traceback.format_exc())

        print(snapshot_data)
        del snapshot_data
