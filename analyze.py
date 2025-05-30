"""Analyze Home Assistant Solar Production."""

import datetime as dt
import sqlite3
from math import ceil
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

# SENSOR_NAME = "sensor.plug1_pv_energy"
SENSOR_ID = 9
# from SELECT * FROM statistics_meta WHERE statistic_id = 'sensor.plug1_pv_energy';
# MAX_KWH_PER_HOUR = 0.63


def read_database() -> pd.DataFrame:
    """
    Read Home Assistant data from SQLite database.

    returns pandas.DataFrame
    """
    con = sqlite3.connect("home-assistant_v2.db")

    # note: column 'state' is reset from time to time, better use 'sum'
    sql = """
    SELECT start_ts, sum as 'kWh_sum'
    FROM statistics
    WHERE statistics.metadata_id = ?
    ORDER BY created_ts ASC
    """
    df = pd.read_sql_query(
        sql, con, params=(SENSOR_ID,)
    )  # using bind variable of hard coded value
    con.close()
    return df


def prepare_df_hours(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare DataFrame of hour sums in local timezone.

    timestamp to datetime
    kWh sum per hour
    """
    # convert timestamp to datetime and use as index
    df["datetime"] = pd.to_datetime(  # convert to datetime
        df["start_ts"],
        unit="s",  # timestamp is in seconds
        utc=True,  # timestamp is in UTC
    ).dt.tz_convert(  # convert to local TZ
        "Europe/Berlin"
    )
    df = df.set_index("datetime")

    # convert continuous meter reading (fortlaufender Zählerstand) to kWh per hour
    df["kWh"] = df["kWh_sum"].shift(-1) - df["kWh_sum"]  # compares to next row

    # remove unnecessary columns
    df = df.drop(columns=["start_ts", "kWh_sum"])

    # print(df)
    return df


def prepare_df_hours_goal_reached(
    df_hour: pd.DataFrame,
    wh_target: int = 100,
) -> pd.DataFrame:
    """
    Analyze how many hours per day did I get more than 100 Wh.

    index: date
    column: hours that reached the target kWh, rolling average of 7 days
    """
    date_last_source = str(df_hour.index[-1].date())  # str to remove timezone

    df = df_hour[df_hour["kWh"] >= wh_target / 1000].copy()
    if len(df) == 0:
        s = f"No hours reached target of {wh_target} Wh."
        raise ValueError(s)

    df["date"] = pd.to_datetime(df.index.date)  # type: ignore
    df = df.groupby(["date"]).agg(count=("kWh", "count"))
    date_last = str(df.index[-1].date())

    if date_last != date_last_source:
        df.loc[pd.to_datetime(date_last_source)] = 0  # type: ignore

    df = df.reindex(
        pd.date_range(df.index.min(), df.index.max(), freq="D"), fill_value=0
    )
    df.index.name = "date"
    df["roll"] = df["count"].rolling(window=7, min_periods=1).mean().round(1)

    # print(df)
    return df


def prepare_df_day(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare DataFrame of day sums from hour-sums.

    date-index
    has columns for year, month, week
    """
    # add column date from DateTime index
    df["date"] = pd.to_datetime(df.index.date)  # type: ignore
    df = df[["kWh", "date"]].groupby(["date"]).sum()
    # add missing dates
    df = df.reindex(
        pd.date_range(df.index.min(), df.index.max(), freq="D"), fill_value=0
    )
    df.index.name = "date"
    df["year"] = df.index.year  # type: ignore
    df["month"] = df.index.month  # type: ignore
    df["week"] = df.index.isocalendar().week  # type: ignore
    # df["month_start"] = df.index - pd.offsets.MonthBegin(1)
    # df["week_start"] = df.index - pd.offsets.Week(weekday=0)

    # print(df)
    return df


def get_date_of_week_start(year, week_number) -> dt.date:  # noqa: ANN001
    """Calc date of week start from year, week-no."""
    date = dt.date.fromisocalendar(int(year), int(week_number), 1)
    return date


def prepare_df_week(df_day: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare DataFrame of week sums.

    date-index
    has kWh sum and day-mean
    has columns for year, week
    """
    df = (
        df_day[["kWh", "year", "week"]]
        .groupby(["year", "week"])
        .agg(kWh_sum=("kWh", "sum"), kWh_mean=("kWh", "mean"))
    )
    df = df.reset_index()

    df["date"] = df.apply(
        lambda row: get_date_of_week_start(row["year"], row["week"]),
        axis=1,
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")

    # print(df)
    return df


def prepare_df_month(df_day: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare DataFrame of month sums.

    date-index
    has kWh sum and day-mean
    has columns for year, month
    """
    df = (
        df_day[["kWh", "year", "month"]]
        .groupby(["year", "month"])
        .agg(kWh_sum=("kWh", "sum"), kWh_mean=("kWh", "mean"))
    )
    df = df.reset_index()

    df["date"] = pd.to_datetime(
        df["year"].astype(str) + "-" + df["month"].astype(str) + "-01"
    )
    df = df.set_index("date")

    # print(df)
    return df


def prepare_df_last_14_days(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare a DataFrame of hours of the last 14 days.
    """
    # do not modify original
    df = df.copy()
    # drop timezone to make it easier
    df.index = df.index.tz_localize(None)  # type: ignore

    # select last 14 days
    df = df[df.index > (df.index[-1] - pd.DateOffset(days=14)).normalize()]
    # TODO: starts at 01:00:00+01:00 instead of 0:00
    df["date"] = pd.to_datetime(df.index.date)  # type: ignore
    last_day = df["date"].iloc[-1]
    df["days_past"] = (last_day - pd.to_datetime(df["date"])).dt.days  # type: ignore
    df["time"] = df.index.time  # type: ignore
    df["hour"] = df.index.hour  # type: ignore

    # print(df)
    return df


def plot_kwh_vs_date(
    df: pd.DataFrame,
    grouper: str,
    kwh_sum: int = 0,
    kwh_max: float = 0,
) -> None:
    """
    Plot kWh per grouper (hour, day, week, month) over all time.
    """
    file_name = f"kWh-date-{grouper}"
    print("plot", file_name)
    plt.suptitle(f"kWh per {grouper}")
    _fig, ax = plt.subplots()
    if grouper in ("hour", "day"):
        df["kWh"].plot(legend=False, drawstyle="steps-post")
    else:
        # kWh as sub-index "mean and sum"
        # ax.step(df["kWh"]["sum"], df["kWh"].index)
        df["kWh_sum"].plot(legend=False, drawstyle="steps-post")

    if kwh_sum > 0:
        ax.text(
            0.99,
            0.99,
            f"total: {kwh_sum} kWh",
            horizontalalignment="right",
            verticalalignment="top",
            transform=ax.transAxes,
        )

    if kwh_max > 0:
        ax.set_ylim(0, kwh_max)
    else:
        ax.set_ylim(
            0,
        )
    plot_format(ax)

    plt.savefig(fname=f"{file_name}.png", format="png")
    plt.close()


def plot_kwh_date_mean(
    df_day: pd.DataFrame,
    df_week: pd.DataFrame,
    df_month: pd.DataFrame,
    kwh_sum: int = 0,
) -> None:
    """Plot kWh per day, week and month (averaged) over all time."""
    file_name = "kWh-date-joined"
    print("plot", file_name)
    _fig, ax = plt.subplots()
    df_day["kWh"].plot(legend=True, drawstyle="steps-post")
    df_week["kWh_mean"].plot(drawstyle="steps-post", linewidth=2.0)
    df_month["kWh_mean"].plot(drawstyle="steps-post", linewidth=3.0)
    plt.legend(["Day", "Week", "Month"])
    plt.suptitle("kWh per Day, averaged per Week and Month")

    if kwh_sum > 0:
        plt.gcf().text(
            0.96,
            0.935,
            f"total: {kwh_sum} kWh",
            horizontalalignment="right",
            verticalalignment="top",
            # transform=ax.transAxes,
        )

    plot_format(ax)
    ax.set_ylim(0, MAX_KWH_PER_DAY)
    plt.ylabel("Kilowatt hours (kWh) per day")

    plt.savefig(fname=f"{file_name}.png", format="png")
    plt.close()


def plot_format(ax) -> None:  # noqa: ANN001
    """Format plots."""
    plt.xlabel("")
    plt.ylabel("kWh")
    plt.grid(axis="both")
    x_tic_locator = mdates.AutoDateLocator(minticks=3, maxticks=7)
    x_tic_formatter = mdates.ConciseDateFormatter(
        x_tic_locator,
        show_offset=True,
        offset_formats=["", "%Y", "%b %Y", "%Y-%b-%d", "%Y-%b-%d", "%Y-%b-%d %H:%M"],
    )
    ax.xaxis.set_major_formatter(x_tic_formatter)
    plt.tight_layout()


def plot_last_14_days(df_hour2: pd.DataFrame) -> None:
    """
    Plot kWh per hour of the last 14 days using subplots.
    """
    days = max(df_hour2["days_past"])
    file_name = f"kWh-hours-last-{days}-days"
    print("plot", file_name)
    fig, ax = plt.subplots(nrows=days, sharex=True, sharey=True, figsize=(4.8, 6.4))
    fig.subplots_adjust(hspace=0)
    plt.suptitle(f"Last {days} days")

    for i in range(days):
        date_to_plot = df_hour2[df_hour2["days_past"] == i]["date"].iloc[0]

        df = df_hour2[df_hour2["days_past"] == i][["kWh", "hour"]]
        df = df.set_index("hour")
        kwh_sum = df["kWh"].sum()

        df.plot.bar(legend=False, ax=ax[i], width=1.0)

        plt.text(
            0.99,
            0.94,
            f"{date_to_plot.strftime('%d.%m.')} {kwh_sum:.1f}kWh",
            horizontalalignment="right",
            verticalalignment="top",
            transform=ax[i].transAxes,
        )
        ax[i].set_xlabel("")
        ax[i].yaxis.set_major_locator(mticker.NullLocator())
        ax[i].yaxis.set_minor_locator(mticker.NullLocator())
    # set same x+y range
    ax[0].set_xlim(4, 20)
    ax[0].set_ylim(0, MAX_KWH_PER_HOUR)
    plt.xticks(rotation=0)

    fig.supxlabel("Hour of the Day")
    fig.supylabel(f"kWh per Hour (max {round(MAX_KWH_PER_HOUR, 1)}kWh)")
    # plt.xlabel("kWh per Hour")
    # plt.ylabel("kWh per Hour")
    # plt.tight_layout()
    plt.subplots_adjust(
        left=None,
        bottom=None,
        right=None,
        top=None,
        wspace=None,
        hspace=None,  # type: ignore
    )

    plt.savefig(fname=f"{file_name}.png", format="png")
    plt.close()


def plot_hours_goal_reached(df: pd.DataFrame, wh_target: int) -> None:
    """
    Plot how many hours per day did I get more than 100Wh.
    """
    file_name = f"hours-of-{wh_target}Wh"
    print("plot", file_name)

    _fig, ax = plt.subplots()
    df.plot(
        legend=False,
        ax=ax,
        # drawstyle="steps-post",
    )
    ax.set_ylim(
        0,
    )
    plot_format(ax)
    plt.ylabel("Count of hours of >= 100kWh")

    plt.savefig(fname=f"{file_name}.png", format="png")
    plt.close()


if __name__ == "__main__":
    # 1. data preparation
    df_hour = prepare_df_hours(read_database())
    df_day = prepare_df_day(df_hour)
    df_week = prepare_df_week(df_day)
    df_month = prepare_df_month(df_day)

    # 1.1 calculations
    KWH_SUM = int(round(df_month["kWh_sum"].sum(), 0))
    MAX_KWH_PER_HOUR = df_hour["kWh"].max()
    MAX_KWH_PER_DAY = df_day["kWh"].max()
    print(f"kWh sum: {KWH_SUM}")
    print(f"kWh max day: {MAX_KWH_PER_DAY:.1f}")

    # 2. other reports
    # 2.1 hours of last 14 days
    df_hour_14d = prepare_df_last_14_days(df_hour)

    # 2.2 how much solar power is consumed, how much is donated to the grid
    CONSUMPTION_WATT_PER_HOUR = 150
    df = df_hour.copy()
    df["kWh_used"] = df["kWh"].clip(upper=CONSUMPTION_WATT_PER_HOUR / 1000)
    df["kWh_donated"] = df["kWh"] - df["kWh_used"]
    print(f"kWh used: {df['kWh_used'].sum():.1f} kWh")
    print(f"kWh donated: {df['kWh_donated'].sum():.1f} kWh")

    # 3. export and plotting
    # 3.1 export
    Path("out").mkdir(exist_ok=True)
    df_day["kWh"].round(3).to_csv("out/day.csv")
    df_week[["kWh_sum", "kWh_mean"]].round(3).to_csv("out/week.csv")
    df_month[["kWh_sum", "kWh_mean"]].round(3).to_csv("out/month.csv")

    # 3.2 plotting
    # add last values for plotting
    today = pd.Timestamp.now().normalize()
    for df in (df_week, df_month):
        last_values = df.iloc[-1]
        df.loc[today] = last_values  # type: ignore
    plot_kwh_vs_date(df_day, "day", kwh_sum=KWH_SUM, kwh_max=ceil(MAX_KWH_PER_DAY))
    plot_kwh_vs_date(df_week, "week", kwh_sum=KWH_SUM)
    plot_kwh_vs_date(df_month, "month", kwh_sum=KWH_SUM)
    plot_kwh_date_mean(df_day, df_week, df_month, kwh_sum=KWH_SUM)
    plot_last_14_days(df_hour_14d)

    # 4. how many hours per day did I reach a certain kWh target
    for wh_target in (50, 100, 200):
        df = prepare_df_hours_goal_reached(df_hour, wh_target=wh_target)
        plot_hours_goal_reached(df, wh_target=wh_target)
