import asyncio
import traceback
from datetime import datetime

import pandas as pd
from aiohttp import ClientSession, TCPConnector
import matplotlib.pyplot as plt

from assets.src.rewards import RequestSnapshot, normalize_timestamp


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
            daily_df.loc[:, 'daily_overall_median'] = daily_df['dag_address_sum'].median()

            daily_df = daily_df[['timestamp', "ordinals", 'destinations', 'dag_address_sum', 'dag_address_mean', 'daily_overall_median']].drop_duplicates('destinations', ignore_index=True)
            list_of_df.append(daily_df)
            t = t - secs

        daily_df = pd.concat(list_of_df, ignore_index=True)
        del list_of_df

        # Calculate the median $DAG earnings for all addresses all days
        overall_daily_median = daily_df['dag_address_mean'].median()

        # TEST PLOT CREATION:
        unique_destinations = daily_df['destinations'].unique()
        path = "assets/data/visualizations"
        for destination in unique_destinations:
            destination_df = daily_df[daily_df['destinations'] == destination]
            plt.style.use('Solarize_Light2')
            fig = plt.figure(figsize=(12, 6))
            fig.patch.set_alpha(0)

            print("style ok")
            plt.plot(pd.to_datetime(destination_df['timestamp'] * 1000, unit='ms'), destination_df['dag_address_mean'], marker='o',
                     label='Daily node earnings')
            plt.plot(pd.to_datetime(destination_df['timestamp'] * 1000, unit='ms'), destination_df['daily_overall_median'], marker='o',
                     label='Daily average network earnings', linestyle=':', alpha=0.5)
            print("plot ok")
            plt.axhline(overall_daily_median, color='red', linestyle='--', label=f'Average network earnings (since {datetime.fromtimestamp(timestamp=timestamp).strftime("%d. %B %Y")})', alpha=0.5)
            plt.xlabel('Time')
            plt.ylabel('$DAG Earnings')
            plt.title('')

            plt.legend()
            plt.xticks(rotation=45)  # Rotate x-axis labels for better readability if needed
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(f"{path}/{destination}.png")
            # plt.show()
            plt.close()
            daily_df['plot'] = f"{path}/{destination}.png"
            # ADD IMAGE PATH TO DATA

        del destination_df
        ###

        daily_df['dag_daily_std_dev'] = daily_df.groupby('destinations')['dag_address_sum'].transform('std')
        daily_df['dag_address_daily_mean'] = daily_df.groupby('destinations')['dag_address_sum'].transform('mean')
        daily_overall_median = daily_df['dag_address_sum'].median()

        # PERFORMING BETTER THAN THE REST ON THE DAILY, IF HIGHER
        daily_df['dag_address_daily_sum_dev'] = daily_df['dag_address_sum'] - daily_overall_median
        daily_df = daily_df[['destinations', 'dag_address_daily_mean', 'dag_address_daily_sum_dev', 'dag_daily_std_dev', 'plot']].drop_duplicates('destinations')
        daily_df['dag_daily_std_dev'].fillna(0, inplace=True)
        daily_df = daily_df.sort_values(by=['dag_address_daily_sum_dev', 'dag_address_daily_mean', 'dag_daily_std_dev'], ascending=[False, False, True]).reset_index(drop=True)
        daily_df['daily_effectivity_score'] = daily_df.index
        print(daily_df)
        input(f'Daily looks okay? ')

        data['dag_address_sum'] = data.groupby('destinations')['dag'].transform('sum')

        # THE USD VALUE NEEDS TO BE MULTIPLIED SINCE IT'S THE VALUE PER DAG
        # data['usd_sum'] = data.groupby('destinations')['usd_value_then'].transform('sum')
        # data['dag_overall_sum_median'] = data['dag_address_sum'].median()

        # PERFORMING BETTER THAN THE REST IN THE TIMESPAN, IF HIGHER
        data['dag_address_sum_dev'] = data['dag_address_sum'] - data['dag_address_sum'].median()
        data = data.merge(daily_df, on='destinations', how='left')
        data = data[['daily_effectivity_score', 'destinations', 'dag_address_sum', 'dag_address_sum_dev', 'dag_address_daily_sum_dev', 'dag_address_daily_mean', 'dag_daily_std_dev', 'plot']].drop_duplicates('destinations')
        del daily_df

        # MOST EFFECTIVE EARNER MUST BE THE LOWEST DAILY STD DEV, THE HIGHEST DAILY MEAN EARNINGS, HIGHEST DAILY SUM DEV
        # (AVERAGE NODE SUM EARNINGS - NETWORK EARNINGS MEDIAN) AND HIGHEST OVERALL SUM DEV (AVERAGE NODE SUM EARNINGS - NETWORK EARNINGS
        # MEDIAN)
        data = data.sort_values(
            by=['dag_address_sum_dev', 'dag_address_daily_sum_dev', 'dag_address_daily_mean', 'dag_daily_std_dev'],
            ascending=[False, False, False, True]).reset_index(
            drop=True)
        # The effectivity score: close to zero is good. F.ex. 6 out of 460 = 6/460*100 = TOP 0.869%;
        # Least spread between daily and overall effectivity
        data['effectivity_score'] = data.index + data['daily_effectivity_score']
        data = data.sort_values(by='dag_address_sum_dev', ascending=False).reset_index(drop=True)
        data['earner_score'] = data.index
        print(data)
