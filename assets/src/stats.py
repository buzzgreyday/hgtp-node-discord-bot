import asyncio
import hashlib
import logging
import traceback
import warnings
from datetime import datetime, timedelta

from bokeh.models import BoxAnnotation
from bokeh.plotting import figure, output_file, save
from bokeh.palettes import Category20c_10
import pandas as pd
import sqlalchemy
from aiohttp import ClientSession, TCPConnector

from assets.src import preliminaries
from assets.src.database.database import post_reward_stats, update_reward_stats, post_metric_stats, update_metric_stats
from assets.src.discord import discord
from assets.src.discord.services import bot
from assets.src.rewards import normalize_timestamp
from assets.src.schemas import RewardStatsSchema, MetricStatsSchema


class Request:
    def __init__(self, session):
        self.session = session

    async def database(self, request_url):
        while True:
            logging.getLogger("stats").info(
                f"stats.py - Requesting database {request_url}"
            )
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
                            f"\tResponse: {response}"
                        )
                        await asyncio.sleep(3)
            except Exception:
                logging.getLogger("stats").error(traceback.format_exc())

    async def validator_endpoint_url(self, request_url):
        while True:
            async with self.session.get(request_url) as response:
                if response.status == 200:
                    data = await response.text()
                    if data:
                        lines = data.split("\n")
                        desired_key = "REACT_APP_DAG_EXPLORER_API_URL"
                        value = None

                        for line in lines:
                            if line.startswith(desired_key):
                                parts = line.split("=")
                                value = parts[1].strip()
                                break

                        return value
                    else:
                        return
                else:
                    logging.getLogger("stats").warning(
                        f"stats.py - Failed getting explorer endpoint info from {request_url}, retrying in 60 seconds"
                    )
                    await asyncio.sleep(60)


sliced_columns = [
    "timestamp",
    "ordinals",
    "destinations",
    "dag_address_daily_sum",
    "daily_overall_median",
    "usd_per_token",
]
final_sliced_columns = [
    "destinations",
    "dag_address_daily_mean",
    "dag_address_daily_sum",
    "daily_overall_median",
    "dag_address_daily_sum_dev",
    "dag_daily_std_dev",
    "usd_address_daily_sum",
]
final_columns = [
    "daily_effectivity_score",
    "destinations",
    "dag_address_sum",
    "daily_overall_median",
    "dag_address_sum_dev",
    "dag_median_sum",
    "dag_address_daily_sum_dev",
    "dag_address_daily_mean",
    "dag_address_daily_sum",
    "dag_daily_std_dev",
    "usd_address_sum",
    "usd_address_daily_sum",
]


def sum_usd(
    df: pd.DataFrame, new_column_name: str, address_specific_sum_column
) -> pd.DataFrame:
    # THE USD VALUE NEEDS TO BE MULTIPLIED SINCE IT'S THE VALUE PER DAG
    df[new_column_name] = df["usd_per_token"] * df[address_specific_sum_column]
    return df


def calculate_address_specific_sum(
    df: pd.DataFrame, new_column_name: str, address_specific_sum_column: str
) -> pd.DataFrame:
    df.loc[:, new_column_name] = df.groupby("destinations")[
        address_specific_sum_column
    ].transform("sum")
    return df


def calculate_address_specific_mean(
    df: pd.DataFrame, new_column_name: str, address_specific_mean_column: str
) -> pd.DataFrame:
    df.loc[:, new_column_name] = df.groupby("destinations")[
        address_specific_mean_column
    ].transform("mean")
    return df


def calculate_address_specific_deviation(
    sliced_snapshot_df: pd.DataFrame,
    new_column_name: str,
    address_specific_sum_column,
    general_sum_column,
) -> pd.DataFrame:
    sliced_snapshot_df[new_column_name] = (
        sliced_snapshot_df[address_specific_sum_column]
        - sliced_snapshot_df[general_sum_column]
    )
    return sliced_snapshot_df


def calculate_address_specific_standard_deviation(
    df: pd.DataFrame, new_column_name: str, address_specific_sum_column: str
) -> pd.DataFrame:
    df[new_column_name] = df.groupby("destinations")[
        address_specific_sum_column
    ].transform("std")
    return df


def calculate_general_data_median(
    df: pd.DataFrame, new_column_name: str, median_column: str
) -> pd.DataFrame:
    df.loc[:, new_column_name] = df[median_column].median()
    return df


def create_timeslice_data(
    data: pd.DataFrame, node_data: pd.DataFrame, start_time: int, travers_seconds: int
):
    """
    TO: Start time is usually the latest available timestamp
    FROM: Traverse seconds is for example seven days, one day or 24 hours in seconds (the time you wish to traverse)
    """
    list_of_daily_snapshot_df = []
    list_of_daily_node_df = []
    while start_time >= data["timestamp"].values.min():
        # Also add 7 days and 24 hours
        sliced_snapshot_df = data[
            (data["timestamp"] >= start_time - travers_seconds)
            & (data["timestamp"] <= start_time)
        ].copy()
        sliced_node_data_df = node_data[
            (node_data["timestamp"] >= start_time - travers_seconds)
            & (node_data["timestamp"] <= start_time)
        ].copy()

        # The following time is a datetime
        sliced_snapshot_df = calculate_address_specific_sum(
            sliced_snapshot_df, "dag_address_daily_sum", "dag"
        )
        sliced_snapshot_df = calculate_general_data_median(
            sliced_snapshot_df, "daily_overall_median", "dag_address_daily_sum"
        )
        # sliced_node_data_df.groupby(['destinations', 'layer', 'public_port'], sort=False)['timestamp'].max()
        # Drop all but the last in the group here and after visualization
        sliced_node_data_df["daily_cpu_load"] = sliced_node_data_df.groupby(
            ["destinations", "layer", "public_port"]
        )["cpu_load_1m"].transform("mean")
        # Clean the data
        sliced_snapshot_df = sliced_snapshot_df[sliced_columns].drop_duplicates(
            "destinations", ignore_index=True
        )
        # Keeping the last grouped row; free disk space, disk space and cpu count
        sliced_node_data_df = sliced_node_data_df.sort_values(
            by="timestamp"
        ).drop_duplicates(
            [
                "destinations",
                "layer",
                "ip",
                "public_port",
            ],
            keep="last",
            ignore_index=True,
        )

        list_of_daily_snapshot_df.append(sliced_snapshot_df)
        list_of_daily_node_df.append(sliced_node_data_df)
        start_time = start_time - travers_seconds

    sliced_snapshot_df = pd.concat(list_of_daily_snapshot_df, ignore_index=True)
    sliced_node_data_df = pd.concat(list_of_daily_node_df, ignore_index=True)
    sliced_snapshot_df = calculate_address_specific_standard_deviation(
        sliced_snapshot_df, "dag_daily_std_dev", "dag_address_daily_sum"
    )
    sliced_snapshot_df = calculate_address_specific_mean(
        sliced_snapshot_df, "dag_address_daily_mean", "dag_address_daily_sum"
    )
    sliced_snapshot_df = calculate_address_specific_deviation(
        sliced_snapshot_df,
        "dag_address_daily_sum_dev",
        "dag_address_daily_sum",
        "daily_overall_median",
    )

    del list_of_daily_snapshot_df, list_of_daily_node_df
    return sliced_snapshot_df, sliced_node_data_df


def create_cpu_visualizations(df: pd.DataFrame, from_timestamp: int):
    """Creates CPU visualizations. However, we need one per IP"""
    unique_destinations = df["destinations"].unique()
    path = "static"
    for destination in unique_destinations:
        logging.getLogger("stats").debug(f"Creating cpu visualization for {destination}")
        output_file(f"{path}/cpu_{destination}.html")
        destination_df = df[df["destinations"] == destination]
        p = figure(
            title=f"",
            x_axis_label="Time",
            y_axis_label="CPU Load Percentage",
            x_axis_type="datetime",
            width=600,
            height=400,
        )
        p.sizing_mode = 'scale_width'
        ports = destination_df["public_port"].unique()
        palette = Category20c_10
        for i, port in enumerate(ports):
            color = palette[i]
            layer_df = destination_df[destination_df["public_port"] == port]

            p.line(
                pd.to_datetime(layer_df["timestamp"] * 1000, unit="ms"),
                layer_df["daily_cpu_load"] / layer_df["cpu_count"] * 100,
                line_width=3,
                legend_label=f"{layer_df['ip'].values[0]}:{layer_df['public_port'].values[0]}",
                color=color,
            )

        p.line(
            pd.to_datetime(df["timestamp"] * 1000, unit="ms"),
            df["daily_cpu_load"].median() / df["cpu_count"].median() * 100,
            line_color="grey",
            line_dash="dashed",
            legend_label="Av. user",
            alpha=0.5,
        )
        green_box = BoxAnnotation(bottom=0, top=80, left=0, fill_alpha=0.1, fill_color='green')
        yellow_box = BoxAnnotation(bottom=80, top=100, left=0, fill_alpha=0.1, fill_color='yellow')
        red_box = BoxAnnotation(bottom=100, left=0, fill_alpha=0.1, fill_color='red')
        p.add_layout(green_box)
        p.add_layout(yellow_box)
        p.add_layout(red_box)
        p.legend.location = "bottom_right"
        p.legend.click_policy = "hide"
        p.legend.label_text_font_size = '8pt'
        p.legend.background_fill_alpha = 0.1

        save(p)


def create_reward_visualizations(df: pd.DataFrame, from_timestamp: int):
    """Creates reward visualizations."""
    unique_destinations = df["destinations"].unique()
    path = "static"
    palette = Category20c_10
    for destination in unique_destinations:
        logging.getLogger("stats").debug(f"Creating reward visualization for {destination}")
        output_file(f"{path}/rewards_{destination}.html")
        destination_df = df[df["destinations"] == destination]
        p = figure(
            title=f"",
            x_axis_label="Time",
            y_axis_label="$DAG Earnings",
            x_axis_type="datetime",
            width=600,
            height=400,
        )
        p.sizing_mode = ('scale_width')
        p.line(
            pd.to_datetime(destination_df["timestamp"] * 1000, unit="ms"),
            destination_df["dag_address_daily_sum"],
            legend_label="Node",
            color=palette[0],
            line_width=3,
        )
        p.line(
            pd.to_datetime(destination_df["timestamp"] * 1000, unit="ms"),
            destination_df["daily_overall_median"],
            legend_label="Network",
            color="silver",
            line_dash="dotted",
            line_width=3,
        )
        p.line(
            pd.to_datetime(destination_df["timestamp"] * 1000, unit="ms"),
            destination_df["dag_address_daily_mean"].median(),
            line_color=palette[0],
            line_dash="dashed",
            legend_label=f"Av. node",
        )

        green_box = BoxAnnotation(bottom=df["daily_overall_median"].median(), left=0, fill_alpha=0.1, fill_color='green')
        red_box = BoxAnnotation(top=df["daily_overall_median"].median(), left=0, fill_alpha=0.1, fill_color='red')
        p.add_layout(green_box)
        p.add_layout(red_box)
        p.legend.location = "bottom_right"
        p.legend.click_policy = "hide"
        p.legend.label_text_font_size = '8pt'
        p.legend.background_fill_alpha = 0.1

        save(p)


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
            snapshot_data = await Request(session).database(
                f"http://127.0.0.1:8000/ordinal/from/{timestamp}"
            )
        except Exception:
            logging.getLogger("stats").error(traceback.format_exc())
            await asyncio.sleep(3)
        else:
            break
    await asyncio.sleep(6)

    while True:
        try:
            node_data = await Request(session).database(
                f"http://127.0.0.1:8000/data/from/{timestamp}"
            )
        except Exception:
            logging.getLogger("stats").error(traceback.format_exc())
            await asyncio.sleep(3)
        else:
            break
    snapshot_data = pd.DataFrame(snapshot_data)
    node_data = pd.DataFrame(node_data)

    return snapshot_data, node_data


async def run():
    await asyncio.sleep(16)
    times = preliminaries.generate_stats_runtimes()
    """
    GET DATA
    """
    logging.getLogger("stats").info(f"Runtimes: {times}")
    while True:
        async with ClientSession(
            connector=TCPConnector(
                # You need to obtain a real (non-self-signed certificate) to run in production
                # ssl=db.ssl_context.load_cert_chain(certfile=ssl_cert_file, keyfile=ssl_key_file)
                # Not intended for production:
                ssl=False
            )
        ) as session:
            current_time = datetime.utcnow().time().strftime("%H:%M:%S")
            try:
                if current_time in times:
                    # Convert to Epoch
                    timestamp = normalize_timestamp(
                        datetime.utcnow().timestamp() - timedelta(days=30).total_seconds()
                    )

                    # From the beginning of time:
                    # timestamp = 1640995200

                    # 30 days in seconds = 2592000
                    # 7 days in seconds = 604800

                    # Raw data calculations: I need the aggregated dag_address_sum from the snapshot data column "dag"
                    snapshot_data, node_data = await get_data(session, timestamp)

                    pd.set_option("display.max_rows", None)
                    pd.options.display.float_format = "{:.2f}".format
                    """
                    CREATE DAILY DATA
                    TO: Start time is the latest available timestamp
                    FROM: The "timestamp" var is the timestamp from where you wish to retrieve data from
                    """
                    start_time = snapshot_data["timestamp"].values.max()
                    # One day in seconds
                    traverse_seconds = 86400
                    sliced_snapshot_df, sliced_node_df = create_timeslice_data(
                        snapshot_data, node_data, start_time, traverse_seconds
                    )

                    # This will be deprecated
                    warnings.filterwarnings("ignore", category=FutureWarning)
                    sliced_snapshot_df["dag_daily_std_dev"].fillna(0, inplace=True)

                    create_reward_visualizations(sliced_snapshot_df, timestamp)
                    create_cpu_visualizations(sliced_node_df, timestamp)

                    # (!) After visual only keep last (sort_values) timestamp and drop duplicates
                    # Keeping the last grouped row; free disk space, disk space and cpu count. You only need the latest timestamp in database
                    # since it's updated daily
                    sliced_node_df = sliced_node_df.sort_values(by="timestamp").drop_duplicates(
                        [
                            "destinations",
                            "layer",
                            "ip",
                            "public_port",
                        ],
                        keep="last",
                        ignore_index=True,
                    )
                    sliced_snapshot_df = sum_usd(
                        sliced_snapshot_df, "usd_address_daily_sum", "dag_address_daily_sum"
                    )
                    # Clean the data
                    sliced_snapshot_df = sliced_snapshot_df[final_sliced_columns].drop_duplicates(
                        "destinations"
                    )

                    """
                    CREATE OVERALL DATA
                    """
                    snapshot_data["dag_address_sum"] = snapshot_data.groupby("destinations")[
                        "dag"
                    ].transform("sum")
                    # Missing dag_address_mean

                    snapshot_data = sliced_snapshot_df.merge(
                        snapshot_data.drop_duplicates("destinations"),
                        on="destinations",
                        how="left",
                    )

                    snapshot_data = sum_usd(snapshot_data, "usd_address_sum", "dag_address_sum")

                    # The node is earning more than the average if sum deviation is positive
                    snapshot_data["dag_address_sum_dev"] = (
                        snapshot_data["dag_address_sum"]
                        - snapshot_data["dag_address_sum"].median()
                    )
                    snapshot_data["dag_median_sum"] = snapshot_data["dag_address_sum"].median()

                    # Delete this (Effectivity score)
                    snapshot_data = snapshot_data.sort_values(
                        by=[
                            "dag_address_sum_dev",
                            "dag_address_daily_sum_dev",
                            "dag_address_daily_mean",
                            "dag_daily_std_dev",
                        ],
                        ascending=[False, False, False, True],
                    ).reset_index(drop=True)


                    snapshot_data = snapshot_data.sort_values(
                        by="dag_address_sum_dev", ascending=False
                    ).reset_index(drop=True)
                    snapshot_data["earner_score"] = snapshot_data.index + 1
                    total_len = len(snapshot_data.index)
                    snapshot_data["count"] = total_len
                    # Initiate the row
                    snapshot_data["percent_earning_more"] = 0.0

                    for i, row in snapshot_data.iterrows():
                        percentage = ((i + 1) / total_len) * 100
                        snapshot_data.at[i, "percent_earning_more"] = percentage
                        row["percent_earning_more"] = percentage
                        reward_data = RewardStatsSchema(**row.to_dict())
                        try:
                            # Post
                            await post_reward_stats(reward_data)
                        except sqlalchemy.exc.IntegrityError:
                            await update_reward_stats(reward_data)

                    for i, row in sliced_node_df.iterrows():
                        key_str = f"{row.id}-{row.ip}-{row.public_port}"
                        hash_index = hashlib.sha256(key_str.encode()).hexdigest()
                        row['hash_index'] = hash_index
                        metric_data = MetricStatsSchema(**row.to_dict())

                        try:
                            # Post
                            await post_metric_stats(metric_data)
                        except Exception:
                            await update_metric_stats(metric_data)

                    del snapshot_data, metric_data
                else:
                    await asyncio.sleep(0.2)

            except Exception as e:
                logging.getLogger("app").error(
                    f"main.py - error: {traceback.format_exc()}"
                )
                await discord.messages.send_traceback(bot, traceback.format_exc())
