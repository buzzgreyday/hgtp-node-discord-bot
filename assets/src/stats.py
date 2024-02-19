import asyncio
import logging
import traceback
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy
from aiohttp import ClientSession, TCPConnector
import matplotlib.pyplot as plt

from assets.src.database.database import post_stats, update_stats
from assets.src.rewards import normalize_timestamp
from assets.src.schemas import RewardStatsSchema


class Request:

    def __init__(self, session):
        self.session = session

    async def database(self, request_url):
        while True:
            logging.getLogger("stats").warning(
                f"stats.py - Requesting database {request_url}")
            try:
                async with self.session.get(request_url, timeout=18000) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data:
                            return data
                        else:
                            return
                    else:
                        logging.getLogger("stats").warning(
                            f"stats.py - Failed getting snapshot data from {request_url}, retrying in 3 seconds:\n"
                            f"\tResponse: {response}")
                        await asyncio.sleep(3)
            except Exception:
                logging.getLogger('stats').error(traceback.format_exc())


    async def validator_endpoint_url(self, request_url):
        while True:
            async with self.session.get(request_url) as response:
                if response.status == 200:
                    data = await response.text()
                    if data:
                        lines = data.split('\n')
                        desired_key = 'REACT_APP_DAG_EXPLORER_API_URL'
                        value = None

                        for line in lines:
                            if line.startswith(desired_key):
                                parts = line.split('=')
                                value = parts[1].strip()
                                break

                        return value
                    else:
                        return
                else:
                    logging.getLogger("stats").warning(f"stats.py - Failed getting explorer endpoint info from {request_url}, retrying in 60 seconds")
                    await asyncio.sleep(60)


sliced_columns = ['timestamp', "ordinals", 'destinations', 'dag_address_daily_sum', 'daily_overall_median', 'usd_per_token']
final_sliced_columns = ['destinations', 'dag_address_daily_mean', 'dag_address_daily_sum', 'daily_overall_median', 'dag_address_daily_sum_dev',
                        'dag_daily_std_dev', 'usd_address_daily_sum']
final_columns = ['daily_effectivity_score', 'destinations', 'dag_address_sum', 'daily_overall_median', 'dag_address_sum_dev', 'dag_median_sum',
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


def calculate_address_specific_deviation(sliced_snapshot_df: pd.DataFrame, new_column_name: str, address_specific_sum_column, general_sum_column) -> pd.DataFrame:
    sliced_snapshot_df[new_column_name] = sliced_snapshot_df[address_specific_sum_column] - sliced_snapshot_df[general_sum_column]
    return sliced_snapshot_df


def calculate_address_specific_standard_deviation(df: pd.DataFrame, new_column_name: str, address_specific_sum_column: str) -> pd.DataFrame:
    df[new_column_name] = df.groupby('destinations')[address_specific_sum_column].transform('std')
    return df

def calculate_general_data_median(df: pd.DataFrame, new_column_name: str, median_column: str) -> pd.DataFrame:
    df.loc[:, new_column_name] = df[median_column].median()
    return df


def create_timeslice_data(data: pd.DataFrame, node_data: pd.DataFrame, start_time: int, travers_seconds: int):
    """
    TO: Start time is usually the latest available timestamp
    FROM: Traverse seconds is for example seven days, one day or 24 hours in seconds (the time you wish to traverse)
    """
    list_of_daily_snapshot_df = []
    list_of_daily_node_df = []
    while start_time >= data['timestamp'].values.min():
        # Also add 7 days and 24 hours
        sliced_snapshot_df = data[(data['timestamp'] >= start_time - travers_seconds) & (data['timestamp'] <= start_time)].copy()
        sliced_node_data_df = node_data[(node_data['timestamp'] >= start_time - travers_seconds) & (node_data['timestamp'] <= start_time)].copy()

        # The following time is a datetime
        sliced_snapshot_df = calculate_address_specific_sum(sliced_snapshot_df, 'dag_address_daily_sum', 'dag')
        sliced_snapshot_df = calculate_general_data_median(sliced_snapshot_df, 'daily_overall_median', 'dag_address_daily_sum')
        # sliced_node_data_df.groupby(['destinations', 'layer', 'public_port'], sort=False)['timestamp'].max()
        # Drop all but the last in the group here and after visualization
        sliced_node_data_df['daily_cpu_load'] = sliced_node_data_df.groupby(['destinations', 'layer', 'public_port'])['cpu_load_1m'].transform('mean')
        # Clean the data
        sliced_snapshot_df = sliced_snapshot_df[sliced_columns].drop_duplicates('destinations', ignore_index=True)
        # Keeping the last grouped row; free disk space, disk space and cpu count
        sliced_node_data_df = sliced_node_data_df.sort_values(by='timestamp').drop_duplicates(['destinations', 'layer', 'ip', 'public_port', ], keep='last', ignore_index=True)
        print(sliced_node_data_df)
        input("Daily okay? ")
        list_of_daily_snapshot_df.append(sliced_snapshot_df)
        list_of_daily_node_df.append(sliced_node_data_df)
        print(f"Timeslice data transformation done, t >= {start_time}!")
        start_time = start_time - travers_seconds

    sliced_snapshot_df = pd.concat(list_of_daily_snapshot_df, ignore_index=True)
    sliced_node_data_df = pd.concat(list_of_daily_node_df, ignore_index=True)
    sliced_snapshot_df = calculate_address_specific_standard_deviation(sliced_snapshot_df, 'dag_daily_std_dev', 'dag_address_daily_sum')
    sliced_snapshot_df = calculate_address_specific_mean(sliced_snapshot_df, 'dag_address_daily_mean', 'dag_address_daily_sum')
    sliced_snapshot_df = calculate_address_specific_deviation(sliced_snapshot_df, 'dag_address_daily_sum_dev',
                                                     'dag_address_daily_sum',
                                                     'daily_overall_median')



    del list_of_daily_snapshot_df, list_of_daily_node_df
    return sliced_snapshot_df, sliced_node_data_df


def create_visualizations(df: pd.DataFrame, from_timestamp: int):
    # Something here is causing a Tkinter related async issue: probably related to .close() or the fact that this was a
    # async function. Look into this.
    unique_destinations = df['destinations'].unique()
    path = "static"
    print("Starting loop")
    for destination in unique_destinations:
        destination_df = df[df['destinations'] == destination]
        plt.style.use('Solarize_Light2')
        fig = plt.figure(figsize=(12, 6), dpi=80)
        try:
            print("style ok")
            plt.plot(pd.to_datetime(destination_df['timestamp'] * 1000, unit='ms'),
                     destination_df['dag_address_daily_sum'], marker='o', color='blue',
                     label='Daily node earnings')
            print("Daily node earnings plot: OK!")
            plt.plot(pd.to_datetime(destination_df['timestamp'] * 1000, unit='ms'),
                     destination_df['daily_overall_median'], marker='o', color='green',
                     label='Daily network earnings', linestyle=':', alpha=0.5)
            print("Daily average network earnings plot: OK!")
            # Don't limit average to any particular address

            plt.axhline(destination_df['dag_address_daily_mean'].median(), color='blue', linestyle='--',
                        label=f'Average daily node earnings (since {datetime.fromtimestamp(timestamp=from_timestamp).strftime("%d. %B %Y")})',
                        alpha=0.5)
            plt.axhline(df['daily_overall_median'].median(), color='green', linestyle='--',
                        label=f'Average daily network earnings (since {datetime.fromtimestamp(timestamp=from_timestamp).strftime("%d. %B %Y")})',
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
        plt.savefig(f"{path}/rewards_{destination}.jpg")
        # plt.show()
        plt.close(fig)


async def get_data(session, timestamp):
    """
    This function requests the necessary data.
    We can get IP and ID from:
    https://d13uswnxs0x35s.cloudfront.net/mainnet/validator-nodes
    https://dyzt5u1o3ld0z.cloudfront.net/mainnet/validator-nodes
    These should be automatically "updated" via this text:
    https://raw.githubusercontent.com/StardustCollective/dag-explorer-v2/main/.env.base
    :param session: aiohttp client session
    :param timestamp: epoch timestamp
    :return: [pd.DataFrame, pd.DataFrame]
    """
    while True:
        try:
            snapshot_data = await Request(session).database(f"http://127.0.0.1:8000/ordinal/from/{timestamp}")
        except Exception:
            logging.getLogger('stats').error(traceback.format_exc())
            print(traceback.format_exc())
            await asyncio.sleep(3)
        else:
            break
    print(f"Got snapshot_data")
    await asyncio.sleep(6)

    while True:
        try:
            node_data = await Request(session).database(f"http://127.0.0.1:8000/data/from/{timestamp}")
        except Exception:
            logging.getLogger('stats').error(traceback.format_exc())
            print(traceback.format_exc())
            await asyncio.sleep(3)
        else:
            break
    snapshot_data = pd.DataFrame(snapshot_data)
    node_data = pd.DataFrame(node_data)

    return snapshot_data, node_data


async def run(configuration):
    await asyncio.sleep(10)

    """
    GET DATA
    """
    logging.getLogger('stats').info("Starting process...")
    async with ClientSession(connector=TCPConnector(
            # You need to obtain a real (non-self-signed certificate) to run in production
            # ssl=db.ssl_context.load_cert_chain(certfile=ssl_cert_file, keyfile=ssl_key_file)
            # Not intended for production:
            ssl=False)) as session:

        # Convert to Epoch
        timestamp = normalize_timestamp(datetime.utcnow().timestamp() - timedelta(days=30).total_seconds())

        # From the beginning of time:
        # timestamp = 1640995200

        # 30 days in seconds = 2592000
        # 7 days in seconds = 604800

        # Raw data calculations: I need the aggregated dag_address_sum from the snapshot data column "dag"
        snapshot_data, node_data = await get_data(session, timestamp)
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
            sliced_snapshot_df, sliced_node_df = create_timeslice_data(snapshot_data, node_data, start_time, traverse_seconds)
        except Exception:
            print(traceback.format_exc())
        sliced_snapshot_df['dag_daily_std_dev'].fillna(0, inplace=True)
        create_visualizations(sliced_snapshot_df, timestamp)
        print('Visualizations done')

        # (!) After visual only keep last (sort_values) timestamp and drop duplicates
        # Keeping the last grouped row; free disk space, disk space and cpu count. You only need the latest timestamp in database
        # since it's updated daily
        sliced_node_df = sliced_node_df.sort_values(by='timestamp').drop_duplicates(
            ['destinations', 'layer', 'ip', 'public_port', ], keep='last', ignore_index=True)
        print(sliced_node_df)
        input("Okay? ")
        try:
            sliced_snapshot_df = sum_usd(sliced_snapshot_df, 'usd_address_daily_sum', 'dag_address_daily_sum')
        except Exception:
            print(traceback.format_exc())
        # Clean the data
        print("Cleaning daily sliced data...")
        sliced_snapshot_df = sliced_snapshot_df[final_sliced_columns].drop_duplicates('destinations')
        print("Cleaning done!")
        print(sliced_snapshot_df)
        input("sliced_snapshot_df looks clean? ")

        """
        CREATE OVERALL DATA
        """
        snapshot_data['dag_address_sum'] = snapshot_data.groupby('destinations')['dag'].transform('sum')
        print(snapshot_data.head(20))
        input("DAG general sum looks okay? ")

        print("Merging daily daily sliced data...")

        try:
            snapshot_data = sliced_snapshot_df.merge(snapshot_data.drop_duplicates('destinations'), on='destinations',
                                            how='left')
            print(snapshot_data)
            input("Merged data looks okay? ")
        except Exception:
            print(traceback.format_exc())
        print("Merging done!")

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

        try:
            snapshot_data = snapshot_data.sort_values(by='dag_address_sum_dev', ascending=False).reset_index(drop=True)
            snapshot_data['earner_score'] = snapshot_data.index + 1
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
                schema = RewardStatsSchema(**row.to_dict())
                try:
                    # Post
                    await post_stats(schema)
                except sqlalchemy.exc.IntegrityError:
                    await update_stats(schema)
        except Exception:
            print(traceback.format_exc())

        print(snapshot_data['earner_score'])
        del snapshot_data
