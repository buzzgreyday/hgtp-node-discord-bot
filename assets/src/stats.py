import asyncio
import hashlib
import logging
import traceback
import warnings
from datetime import datetime, timedelta, timezone

from bokeh.models import BoxAnnotation, Range1d, LinearAxis, HoverTool, Legend, ColumnDataSource
from bokeh.plotting import figure, output_file, save
from bokeh.palettes import Category20c_10
import pandas as pd
import sqlalchemy
from scipy import stats
from aiohttp import ClientSession, TCPConnector

from assets.src import preliminaries
from assets.src.database.database import post_reward_stats, update_reward_stats, post_metric_stats, update_metric_stats, \
    delete_rows_not_in_new_data
from assets.src.discord import discord, messages
from assets.src.discord.services import bot
from assets.src.rewards import normalize_timestamp
from assets.src.schemas import RewardStatsSchema, MetricStatsSchema

"""CLASSES"""


class Request:
    def __init__(self, session):
        self.session = session

    async def database(self, request_url):
        logging.getLogger("stats").info(
            f"stats.py - Requesting database {request_url}"
        )
        while True:
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


class Visual:
    def __init__(self, data: pd.DataFrame):
        self.dark_theme_bg_color = "#3d3d3d"
        self.dark_theme_text_color = "#f1f2f2"
        self.dark_theme_highlight_active_color = "#00b4cd"
        self.dark_theme_highlight_inactive_color = "#007484"
        self.df = data

    def add_color(self, p):
        # Set background fill color to dark
        p.title.text_color = self.dark_theme_text_color
        p.background_fill_color = self.dark_theme_bg_color
        p.border_fill_color = self.dark_theme_bg_color
        p.outline_line_color = self.dark_theme_bg_color
        p.xaxis.axis_label_text_color = self.dark_theme_text_color
        p.yaxis.axis_label_text_color = self.dark_theme_text_color
        p.xaxis.major_label_text_color = self.dark_theme_text_color
        p.yaxis.major_label_text_color = self.dark_theme_text_color

        return p

    def reward_plot(self):
        try:
            daily_network_average = self.df["daily_overall_median"].mean()
            unique_destinations = self.df["destinations"].unique()
            path = "static"
            palette = Category20c_10

            for destination in unique_destinations:
                logging.getLogger("stats").debug(f"Creating reward visualization for {destination}")
                output_file(f"{path}/rewards_{destination}.html")
                destination_df = self.df[self.df["destinations"] == destination].copy()

                # Convert timestamp to datetime for plotting
                destination_df['datetime'] = pd.to_datetime(destination_df["timestamp"], unit="s")
                destination_df['daily_network_average'] = daily_network_average

                # Prepare data for plotting using ColumnDataSource
                source = ColumnDataSource(destination_df)

                # Get the closest timestamp for zoom
                start_x_zoom = destination_df['datetime'].max() - pd.Timedelta(days=7)
                end_x_zoom = destination_df['datetime'].max() + pd.Timedelta(days=1)

                p = figure(
                    title="",
                    x_axis_label="Time (UTC)",
                    y_axis_label="$DAG Earnings (daily)",
                    x_axis_type="datetime",
                    # width=600,
                    # height=600,
                    x_range=[start_x_zoom, end_x_zoom],
                    toolbar_location='above'
                )
                p.sizing_mode = 'scale_both'
                p.xaxis.major_label_orientation = 0.785
                p.toolbar.logo = None

                # Plot DAG Earnings (Node)
                line_node = p.line(
                    'datetime', 'dag_address_daily_sum',
                    source=source,
                    color=palette[0],
                    line_width=3,
                )

                # Plot Network Median
                line_network = p.line(
                    'datetime', 'daily_overall_median',
                    source=source,
                    color="silver",
                    line_dash="dotted",
                    line_width=3,
                )

                # Plot Node Average
                line_node_avg = p.line(
                    'datetime', 'dag_address_daily_mean',
                    source=source,
                    line_color=palette[0],
                    line_dash="dashed",
                )

                # Add a second y-axis for USD Value
                p.extra_y_ranges = {"usd_value": Range1d(start=0, end=destination_df["usd_address_daily_sum"].max())}
                p.add_layout(LinearAxis(y_range_name="usd_value", axis_label="$USD Value (daily)"), 'right')

                # Line for the right y-axis
                line_usd = p.line(
                    'datetime', 'usd_address_daily_sum',
                    source=source,
                    color="green",
                    line_width=3,
                    y_range_name="usd_value"
                )

                # Create and add the legend below the plot
                legend1 = Legend(items=[
                    ("Node Earnings ($DAG)", [line_node]),
                    ("Node Earnings Value ($USD)", [line_usd])

                ], orientation="horizontal", location="center")
                legend2 = Legend(items=[
                    ("Network Earnings ($DAG)", [line_network]),
                    ("Node Avg. Earnings ($DAG)", [line_node_avg])

                ], orientation="horizontal", location="center")
                legend1.label_text_color = self.dark_theme_text_color
                legend2.label_text_color = self.dark_theme_text_color
                legend1.background_fill_color = self.dark_theme_highlight_inactive_color
                legend2.background_fill_color = self.dark_theme_highlight_inactive_color
                legend1.border_line_color = self.dark_theme_bg_color
                legend2.border_line_color = self.dark_theme_bg_color
                legend1.inactive_fill_color = self.dark_theme_bg_color
                legend2.inactive_fill_color = self.dark_theme_bg_color
                legend1.padding = 0  # Set the padding inside the legend box to 0
                legend1.margin = 0  # Set the margin around the legend box to 0
                legend2.padding = 0  # Set the padding inside the legend box to 0
                legend2.margin = 0  # Set the margin around the legend box to 0
                legend1.click_policy = "hide"
                legend2.click_policy = "hide"
                p.add_layout(legend1, 'below')
                p.add_layout(legend2, 'below')
                # Create the invisible renderer
                invisible_line = p.line(
                    'datetime', 'daily_network_average',
                    source=source,
                    line_color="white",  # Set color to blend in with the background
                    line_alpha=0,  # Make it completely transparent
                    line_width=0,  # Set width to 0
                )

                # Setup the hover tool
                hover = HoverTool(
                    renderers=[invisible_line],
                    tooltips=
                    """
                    <div style="background-color: #3d3d3d; padding: 3px; border: none;">
                        <span style="font-size: 0.8em; font-weight: bold; color: #f1f2f2;">Date</span>
                        <span style="font-size: 0.8em; color: #00b4cd;">@datetime{%Y-%m-%d %H:%M:%S}</span><br>
                        <span style="font-size: 0.8em; font-weight: bold; color: #f1f2f2;">Node Earnings (DAG)</span>
                        <span style="font-size: 0.8em; color: #00b4cd;">@dag_address_daily_sum{0.2f}</span><br>
                        <span style="font-size: 0.8em; font-weight: bold; color: #f1f2f2;">Node Earnings (USD)</span>
                        <span style="font-size: 0.8em; color: #00b4cd;">@usd_address_daily_sum{0.2f}</span><br>
                        <span style="font-size: 0.8em; font-weight: bold; color: #f1f2f2;">Network Earnings (DAG)</span>
                        <span style="font-size: 0.8em; color: #00b4cd;">@daily_overall_median{0.2f}</span><br>
                        <span style="font-size: 0.8em; font-weight: bold; color: #f1f2f2;">Node Average (DAG)</span>
                        <span style="font-size: 0.8em; color: #00b4cd;">@dag_address_daily_mean{0.2f}</span><br>
                        <span style="font-size: 0.8em; font-weight: bold; color: #f1f2f2;">Network Average (DAG)</span>
                        <span style="font-size: 0.8em; color: #00b4cd;">@daily_network_average{0.2f}</span><br>
                    """,
                    # [
                    #     ("Date", "@datetime{%Y-%m-%d %H:%M:%S}"),
                    #     ("Node Earnings ($DAG)", "@dag_address_daily_sum{0.2f}"),
                    #     ("Node Earnings ($)", "@usd_address_daily_sum{0.2f}"),
                    #     ("Network Earnings ($DAG)", "@daily_overall_median{0.2f}"),
                    #     ("Node Average ($DAG)", "@dag_address_daily_mean{0.2f}"),
                    #     ("Network Average ($DAG)", "@daily_network_average{0.2f}")
                    # ],
                    formatters={"@datetime": "datetime"},
                    mode="mouse"
                )
                p.add_tools(hover)

                # Apply custom color modifications (if any)
                green_box = BoxAnnotation(bottom=daily_network_average, left=0, fill_alpha=0.1,
                                          fill_color='#00b4cd')
                red_box = BoxAnnotation(top=daily_network_average, left=0, fill_alpha=0.1,
                                        fill_color='#f64336')
                p.add_layout(green_box)
                p.add_layout(red_box)

                p = self.add_color(p)

                # Save the plot to a file
                save(p)
        except Exception:
            logging.getLogger("stats").critical(traceback.format_exc())

    def cpu_plot(self):
        unique_destinations = self.df["destinations"].unique()
        path = "static"
        for destination in unique_destinations:
            logging.getLogger("stats").debug(f"Creating cpu visualization for {destination}")
            output_file(f"{path}/cpu_{destination}.html")
            destination_df = self.df[self.df["destinations"] == destination]
            p = figure(
                title=f"",
                x_axis_label="Time (UTC)",
                y_axis_label="CPU Load Percentage (daily)",
                x_axis_type="datetime",
                width=600,
                height=400,
            )
            p.sizing_mode = 'scale_width'

            # Set the text color for the legend (none here)
            # p.legend.label_text_color = "#f1f2f2"
            ports = destination_df["public_port"].unique()
            palette = Category20c_10
            for i, port in enumerate(ports):
                color = palette[i]
                layer_df = destination_df[destination_df["public_port"] == port]

                p.line(
                    pd.to_datetime(layer_df["timestamp"], unit="s"),
                    layer_df["daily_cpu_load"] / layer_df["cpu_count"] * 100,
                    line_width=3,
                    legend_label=f"{layer_df['ip'].values[0]}:{layer_df['public_port'].values[0]}",
                    color=color,
                )

            p.line(
                pd.to_datetime(self.df["timestamp"], unit="s"),
                self.df["daily_cpu_load"].median() / self.df["cpu_count"].median() * 100,
                line_color="grey",
                line_dash="dashed",
                legend_label="Nodebot User Avg.",
                alpha=0.5,
            )
            green_box = BoxAnnotation(bottom=0, top=80, left=0, fill_alpha=0.1, fill_color='#00b4cd')
            yellow_box = BoxAnnotation(bottom=80, top=100, left=0, fill_alpha=0.1, fill_color='yellow')
            red_box = BoxAnnotation(bottom=100, left=0, fill_alpha=0.1, fill_color='#f64336')
            p.add_layout(green_box)
            p.add_layout(yellow_box)
            p.add_layout(red_box)

            p = self.add_color(p)

            save(p)


"""PANDAS COLUMNS"""

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
    # This is now filtered mean, not median
    "dag_median_sum",
    "dag_address_daily_sum_dev",
    "dag_address_daily_mean",
    "dag_address_daily_sum",
    "dag_daily_std_dev",
    "usd_address_sum",
    "usd_address_daily_sum",
]

"""FUNCTIONS"""


def _sum_usd(
        df: pd.DataFrame, new_column_name: str, address_specific_sum_column
) -> pd.DataFrame:
    # THE USD VALUE NEEDS TO BE MULTIPLIED SINCE IT'S THE VALUE PER DAG :)
    df[new_column_name] = df["usd_per_token"] * df[address_specific_sum_column]
    return df


def _calculate_address_specific_sum(
        df: pd.DataFrame, new_column_name: str, address_specific_sum_column: str
) -> pd.DataFrame:
    df.loc[:, new_column_name] = df.groupby("destinations")[
        address_specific_sum_column
    ].transform("sum")
    return df


def _calculate_address_specific_mean(
        df: pd.DataFrame, new_column_name: str, address_specific_mean_column: str
) -> pd.DataFrame:
    df.loc[:, new_column_name] = df.groupby("destinations")[
        address_specific_mean_column
    ].transform("mean")
    return df


def _calculate_address_specific_deviation(
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


def _calculate_address_specific_standard_deviation(
        df: pd.DataFrame, new_column_name: str, address_specific_sum_column: str
) -> pd.DataFrame:
    df[new_column_name] = df.groupby("destinations")[
        address_specific_sum_column
    ].transform("std")
    return df


def _calculate_general_data_median(
        df: pd.DataFrame, new_column_name: str, median_column: str
) -> pd.DataFrame:
    df.loc[:, new_column_name] = df[median_column].median()
    return df


def _traverse_slice_snapshot_data(data: pd.DataFrame, start_time, traverse_seconds):
    # Slice daily data
    sliced_snapshot_df = data[
        (data["timestamp"] >= start_time - traverse_seconds)
        & (data["timestamp"] <= start_time)
        ].copy()
    if not sliced_snapshot_df.empty:
        # Sum together the daily amount of $DAG earned by every individual operator
        # and create the column "dag_address_daily_sum"

        sliced_snapshot_df = _calculate_address_specific_sum(
            sliced_snapshot_df, "dag_address_daily_sum", "dag"
        )

        # Calculate the daily median of all $DAG earned by all operators

        sliced_snapshot_df = _calculate_general_data_median(
            sliced_snapshot_df, "daily_overall_median", "dag_address_daily_sum"
        )
        # Clean snapshot data rows
        sliced_snapshot_df = sliced_snapshot_df[sliced_columns].drop_duplicates(
            "destinations", ignore_index=True
        )
        return sliced_snapshot_df


def _traverse_slice_node_data(data: pd.DataFrame, start_time, traverse_seconds):
    sliced_node_data_df = data[
        (data["timestamp"] >= start_time - traverse_seconds)
        & (data["timestamp"] <= start_time)
        ].copy()

    # Calculate the average daily CPU for each operator instance
    sliced_node_data_df["daily_cpu_load"] = sliced_node_data_df.groupby(
        ["destinations", "layer", "public_port"]
    )["cpu_load_1m"].transform("mean")

    # Clean CPU data rows but keep the last grouped row.
    # Last row should be used to save most recent free space and in case an operator upgraded disk or CPUs.
    # This might later look like a duplicated clean-up, but it isn't.
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
    return sliced_node_data_df


def _calculate_generals_post_traverse_slice(sliced_snapshot_df: pd.DataFrame):
    # Calculate the daily rewards standard deviation per wallet
    sliced_snapshot_df = _calculate_address_specific_standard_deviation(
        sliced_snapshot_df, "dag_daily_std_dev", "dag_address_daily_sum"
    )
    # set None to 0
    sliced_snapshot_df["dag_daily_std_dev"].fillna(0, inplace=True)
    # Calculate the daily average rewards received per wallet
    sliced_snapshot_df = _calculate_address_specific_mean(
        sliced_snapshot_df, "dag_address_daily_mean", "dag_address_daily_sum"
    )
    # Calculate how much the daily rewards received per address deviates from the average of all daily rewards received
    # by operators
    sliced_snapshot_df = _calculate_address_specific_deviation(
        sliced_snapshot_df,
        "dag_address_daily_sum_dev",
        "dag_address_daily_sum",
        "daily_overall_median",
    )
    return sliced_snapshot_df


def _create_timeslice_data(
        data: pd.DataFrame, node_data: pd.DataFrame, start_time: int, traverse_seconds: int = 86400
):
    """
    COULD USE SOME CLEANING
    TO: Start time is usually the latest available timestamp
    FROM: Traverse seconds is for example one day (default) seven days, one day or 24 hours in seconds (the time you wish to traverse)
    """
    list_of_daily_snapshot_df = []
    list_of_daily_node_df = []

    while start_time >= data["timestamp"].values.min():
        # Add daily data to the chain of daily data before traversing to the day before
        list_of_daily_snapshot_df.append(_traverse_slice_snapshot_data(data, start_time, traverse_seconds))
        list_of_daily_node_df.append(_traverse_slice_node_data(node_data, start_time, traverse_seconds))
        # Set start_time to the day before and continue loop
        start_time = start_time - traverse_seconds

    # When timestamp is over 30 days old create a new dfs containing the daily sliced data
    try:
        sliced_snapshot_df = pd.concat(list_of_daily_snapshot_df, ignore_index=True)
    except ValueError:
        sliced_snapshot_df = pd.DataFrame()
    else:
        sliced_snapshot_df = _calculate_generals_post_traverse_slice(sliced_snapshot_df)
    try:
        sliced_node_data_df = pd.concat(list_of_daily_node_df, ignore_index=True)
    except ValueError:
        sliced_node_data_df = pd.DataFrame()

    # Return the data containing cleaner daily data
    return sliced_snapshot_df, sliced_node_data_df


async def _get_data(timestamp):
    """
    This function requests the necessary data.
    We can get IP and ID from:
    https://d13uswnxs0x35s.cloudfront.net/mainnet/validator-nodes
    https://dyzt5u1o3ld0z.cloudfront.net/mainnet/validator-nodes
    These should be automatically "updated" via this text:
    https://raw.githubusercontent.com/StardustCollective/dag-explorer-v2/main/.env.base
    :param timestamp: epoch timestamp
    :return: [pd.DataFrame, pd.DataFrame]
    """
    async with ClientSession(
            connector=TCPConnector(
                # You need to obtain a real (non-self-signed certificate) to run in production
                # ssl=db.ssl_context.load_cert_chain(certfile=ssl_cert_file, keyfile=ssl_key_file)
                # Not intended for production:
                ssl=False
            )
    ) as session:
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
    async with ClientSession(
            connector=TCPConnector(
                # You need to obtain a real (non-self-signed certificate) to run in production
                # ssl=db.ssl_context.load_cert_chain(certfile=ssl_cert_file, keyfile=ssl_key_file)
                # Not intended for production:
                ssl=False
            )
    ) as session:
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


def _generate_visuals(sliced_snapshot_df, sliced_node_df):
    # set None to 0
    sliced_node_df["daily_cpu_load"].fillna(0, inplace=True)
    sliced_snapshot_df["dag_address_daily_sum"].fillna(0, inplace=True)
    # Calculate the USD value of the daily earnings per node wallet
    sliced_snapshot_df = _sum_usd(
        sliced_snapshot_df, "usd_address_daily_sum", "dag_address_daily_sum"
    )
    # Create visual representations of the daily data
    Visual(sliced_snapshot_df).reward_plot()
    Visual(sliced_node_df).cpu_plot()

    """CLEAN"""
    # This might seem like a duplication from the clean-up in "creat_timeslice_data,
    # but it isn't. After having created the CPU visuals we don't need all daily CPU data per operator,
    # we only need the latest daily data (today) for updating the database CPU table, since data is
    # updated daily.
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
    # Clean the daily snapshot data columns
    sliced_snapshot_df = sliced_snapshot_df[final_sliced_columns].drop_duplicates(
        "destinations"
    )
    return sliced_snapshot_df, sliced_node_df


def _remove_extreme_outliers(snapshot_data: pd.DataFrame):
    # Define a threshold for the Z-score (positive numbers only)
    zscore_threshold = 0.30
    zscore_threshold_tolerance = 0.005
    snapshot_data["dag_address_sum_zscore"] = stats.zscore(snapshot_data.dag_address_sum)
    # Filter out rows where z-score exceeds the threshold by taking the absolute:
    # treat both positive and negative deviations from the mean in the same manner
    filtered_df = snapshot_data[(snapshot_data[
                                     'dag_address_sum_zscore'].abs() <= zscore_threshold + zscore_threshold_tolerance) & (
                                        snapshot_data[
                                            'dag_address_sum_zscore'].abs() >= 0)].copy()
    # Use .copy() to ensure a new DataFrame is created, preventing chained assignments
    return filtered_df


async def run():
    """
    Initiate the statistics process
    :return:
    """
    await asyncio.sleep(16)
    times = preliminaries.generate_stats_runtimes()
    logging.getLogger("stats").info(f"Runtimes: {times}")
    while True:
        current_time = datetime.now(timezone.utc).time().strftime("%H:%M:%S")
        try:
            if current_time in times:
                """SETTINGS"""
                pd.set_option("display.max_rows", None)
                warnings.filterwarnings("ignore", category=FutureWarning)
                pd.options.display.float_format = "{:.2f}".format
                # Convert timestamp to epoch
                timestamp = normalize_timestamp(
                    datetime.now(timezone.utc).timestamp() - timedelta(days=30).total_seconds()
                )
                """GET DATA"""
                # Important: The original data requested below is used after creation of daily data.
                # Therefore, do not delete the data before updating the database.
                #
                # The df "snapshot_data" is the rewards used to calc reward statistics and "node_data" is used to
                # calc CPU statistics
                while True:
                    snapshot_data, node_data = await _get_data(timestamp)
                    if not snapshot_data.empty and not node_data.empty:
                        break
                    else:
                        await asyncio.sleep(30)

                """
                    CREATE DAILY DATA
                    TO: Start time is the latest available timestamp
                    FROM: The "timestamp" var is the timestamp from where you wish to retrieve data from
                    """

                # Slice snapshot and node data into daily data
                sliced_snapshot_df, sliced_node_df = _create_timeslice_data(
                    snapshot_data, node_data, snapshot_data["timestamp"].values.max()
                )
                if sliced_snapshot_df.empty or sliced_node_df.empty:
                    logging.getLogger("stats").error(f"sliced data is None:\n"
                                                     f"\t{sliced_snapshot_df}\n"
                                                     f"\t{sliced_node_df}")
                    await asyncio.sleep(60)
                    continue
                sliced_snapshot_df, sliced_node_df = _generate_visuals(sliced_snapshot_df, sliced_node_df)

                """CREATE DATA FOR THE ENTIRE PERIOD"""
                # Use the unsliced data to calculate the sum of all $DAG earned per node wallet
                snapshot_data["dag_address_sum"] = snapshot_data.groupby("destinations")[
                    "dag"
                ].transform("sum")

                # Merge the daily data into the original unsliced snapshot data
                snapshot_data = sliced_snapshot_df.merge(
                    snapshot_data.drop_duplicates("destinations"),
                    on="destinations",
                    how="left",
                )
                # Calculate USD value of total amount of $DAG earned per node wallet in the period.
                # If 30 days then 86400
                snapshot_data = _sum_usd(snapshot_data, "usd_address_sum", "dag_address_sum")

                filtered_df = _remove_extreme_outliers(snapshot_data)

                # THIS WILL NEED TO BE RENAMED. NOT USING THE MEDIAN ANYMORE, BUT FILTERED MEAN:
                snapshot_data.loc[:, "dag_median_sum"] = filtered_df["dag_address_sum"].mean()

                # The node is earning more than the average if sum deviation is positive, less if negative
                snapshot_data["dag_address_sum_dev"] = (
                        snapshot_data["dag_address_sum"]
                        - snapshot_data["dag_median_sum"]
                )

                # Order the data by top earners
                snapshot_data = snapshot_data.sort_values(
                    by="dag_address_sum", ascending=False
                ).reset_index(drop=True)
                snapshot_data["earner_score"] = snapshot_data.index + 1
                # Total len is used to count the total number of nodes and calc the percent of node wallets
                # earning more than each individual node wallet
                total_len = len(snapshot_data.index)
                # Count total number of node wallets earning rewards in the period
                snapshot_data["count"] = total_len
                # Calculate the percentage of node wallets earning more than each individual node wallet.
                # Start by preparing the new data column
                snapshot_data["percent_earning_more"] = 0.0

                # Initialize new columns with 0.0
                snapshot_data.loc[:, "above_dag_address_earner_highest"] = 0.0
                snapshot_data.loc[:, "above_dag_addresses_earnings_mean"] = 0.0
                snapshot_data.loc[:, "above_dag_address_earnings_deviation_from_mean"] = 0.0
                snapshot_data.loc[:, "above_dag_address_earnings_from_highest"] = 0.0
                snapshot_data.loc[:, "above_dag_address_earnings_std_dev"] = 0.0
                snapshot_data.loc[:, "nonoutlier_dag_addresses_minted_sum"] = filtered_df["dag_address_sum"].sum()

                # Loop through DataFrame rows using iterrows()

                for i, row in filtered_df.iterrows():
                    df = filtered_df[filtered_df.dag_address_sum > row.dag_address_sum]
                    # Update each row individually
                    snapshot_data.loc[
                        snapshot_data.destinations == row.destinations, "above_dag_address_earner_highest"] = df.dag_address_sum.max()
                    snapshot_data.loc[
                        snapshot_data.destinations == row.destinations, "above_dag_addresses_earnings_mean"] = df.dag_address_sum.mean()
                    snapshot_data.loc[
                        snapshot_data.destinations == row.destinations, "above_dag_address_earnings_deviation_from_mean"] = df.dag_address_sum.mean() - row.dag_address_sum
                    snapshot_data.loc[
                        snapshot_data.destinations == row.destinations, "above_dag_address_earnings_from_highest"] = df.dag_address_sum.max() - row.dag_address_sum
                    snapshot_data.loc[
                        snapshot_data.destinations == row.destinations, "above_dag_address_earnings_std_dev"] = df.dag_address_sum.std()

                # Calculate percentage earning more and then save reward data to database, row-by-row.
                for i, row in snapshot_data.iterrows():
                    percentage = ((i + 1) / total_len) * 100
                    # Add the new data to the reward data for the entire period
                    snapshot_data.at[i, "percent_earning_more"] = percentage
                    # Add the new data to the reward database entry
                    row["percent_earning_more"] = percentage
                    # Validate the data
                    try:
                        d = row.to_dict()
                        reward_data = RewardStatsSchema(**d)
                    except Exception:
                        logging.getLogger("stats").critical(traceback.format_exc())
                        continue
                    try:
                        # Post data if no data exists
                        await post_reward_stats(reward_data)
                    except sqlalchemy.exc.IntegrityError:
                        # Update data, if data already exists
                        await update_reward_stats(reward_data)
                    except Exception:
                        logging.getLogger("stats").critical(traceback.format_exc())

                # Upload metrics (CPU) data to database. Since every wallet can be associated with multiple node
                # instances and different server specifications, we'll create a hash to properly update the
                # database.
                new_data = []
                for i, row in sliced_node_df.iterrows():

                    key_str = f"{row.id}-{row.ip}-{row.public_port}"
                    hash_index = hashlib.sha256(key_str.encode()).hexdigest()
                    row['hash_index'] = hash_index
                    metric_data = MetricStatsSchema(**row.to_dict())
                    new_data.append(row.to_dict())
                    try:
                        # Post data if no data exists
                        await post_metric_stats(metric_data)
                    except sqlalchemy.exc.IntegrityError:
                        # Update data, if data already exists
                        await update_metric_stats(metric_data)
                    except Exception:
                        logging.getLogger("stats").critical(traceback.format_exc())
                # Delete entries not present in the sliced_node_df
                await delete_rows_not_in_new_data(new_data)
                # After saving data, give GIL something to do.
                # del snapshot_data, metric_data
            else:
                await asyncio.sleep(0.2)

        except Exception as e:
            # If anything fails, send a traceback to the creator
            logging.getLogger("app").error(
                f"main.py - error: {traceback.format_exc()}"
            )
            await messages.send_traceback(bot, traceback.format_exc())
