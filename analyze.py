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
    kWh_target: float = 0.100,  # noqa: N803
) -> pd.DataFrame:
    """
    Analyze how many hours per day did I get more than 100 Wh.

    index: date
    column: hours that reached the target kWh, rolling average of 7 days
    """
    date_last_source = str(df_hour.index[-1].date())  # str to remove timezone

    df = df_hour[df_hour["kWh"] >= kWh_target].copy()

    df["date"] = pd.to_datetime(df.index.date)  # type: ignore
    df = df.groupby(["date"]).agg(count=("kWh", "count"))
    date_last = str(df.index[-1].date())

    if date_last != date_last_source:
        df.loc[pd.to_datetime(date_last_source)] = 0

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
    # dropping timezone to make it easier
    df.index = df.index.tz_localize(None)  # type: ignore

    # df.index = df.index.tz_localize(None)
    df = df[df.index > (df.index[-1] - pd.DateOffset(days=14)).normalize()]
    # TODO: starts at 01:00:00+01:00 instead of 0:00
    df["date"] = pd.to_datetime(df.index.date)  # type: ignore
    # today = pd.Timestamp.now().normalize()
    last_day = df["date"].iloc[-1]
    df["days_past"] = (last_day - pd.to_datetime(df["date"])).dt.days  # type: ignore
    df["time"] = df.index.time  # type: ignore
    df["hour"] = df.index.hour  # type: ignore
    # print(df)
    return df


def plot_kWh(  # noqa: N802
    df: pd.DataFrame,
    grouper: str,
    kWh_sum: int = 0,  # noqa: N803
    kWh_max: float = 0,  # noqa: N803
) -> None:
    """
    Plot kWh per grouper (hour, day, week, month) over all time.
    """
    file_name = f"kWh-date-{grouper}"
    print("plot", file_name)
    plt.suptitle(f"kWh per {grouper}")
    fig, ax = plt.subplots()
    if grouper in ("hour", "day"):
        df["kWh"].plot(legend=False, drawstyle="steps-post")
    else:
        # kWh as sub-index "mean and sum"
        # ax.step(df["kWh"]["sum"], df["kWh"].index)
        df["kWh_sum"].plot(legend=False, drawstyle="steps-post")

    if kWh_sum > 0:
        ax.text(
            0.99,
            0.99,
            f"total: {kWh_sum} kWh",
            horizontalalignment="right",
            verticalalignment="top",
            transform=ax.transAxes,
        )

    if kWh_max > 0:
        ax.set_ylim(0, kWh_max)
    else:
        ax.set_ylim(
            0,
        )
    plot_format(ax)

    plt.savefig(fname=f"{file_name}.png", format="png")
    plt.close()


def plot_kWh_date_mean(  # noqa: N802
    df_day: pd.DataFrame,
    df_week: pd.DataFrame,
    df_month: pd.DataFrame,
    kWh_sum: int = 0,  # noqa: N803
) -> None:
    """Plot kWh per day, week and month (averaged) over all time."""
    file_name = "kWh-date-joined"
    print("plot", file_name)
    fig, ax = plt.subplots()
    df_day["kWh"].plot(legend=True, drawstyle="steps-post")
    df_week["kWh_mean"].plot(drawstyle="steps-post", linewidth=2.0)
    df_month["kWh_mean"].plot(drawstyle="steps-post", linewidth=3.0)
    plt.legend(["Day", "Week", "Month"])
    plt.suptitle("kWh per Day, averaged per Week and Month")

    if kWh_sum > 0:
        plt.gcf().text(
            0.96,
            0.935,
            f"total: {kWh_sum} kWh",
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
    days = 14
    file_name = f"kWh-hours-last-{days}-days"
    print("plot", file_name)
    fig, ax = plt.subplots(nrows=days, sharex=True, sharey=True, figsize=(4.8, 6.4))
    fig.subplots_adjust(hspace=0)
    plt.suptitle(f"Last {days} days")

    for i in range(days):
        date_to_plot = df_hour2[df_hour2["days_past"] == i]["date"].iloc[0]

        df = df_hour2[df_hour2["days_past"] == i][["kWh", "hour"]]
        df = df.set_index("hour")
        kWh_sum = df["kWh"].sum()  # noqa: N806

        df.plot.bar(legend=False, ax=ax[i], width=1.0)

        plt.text(
            0.99,
            0.94,
            f'{date_to_plot.strftime("%d.%m.")} {kWh_sum:.1f}kWh',
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
    fig.supylabel(f"kWh per Hour (max {round(MAX_KWH_PER_HOUR,1)}kWh)")
    # plt.xlabel("kWh per Hour")
    # plt.ylabel("kWh per Hour")
    # plt.tight_layout()
    plt.subplots_adjust(
        left=None, bottom=None, right=None, top=None, wspace=None, hspace=None
    )

    plt.savefig(fname=f"{file_name}.png", format="png")
    plt.close()


def plot_hours_goal_reached(df: pd.DataFrame, Wh_target: int) -> None:  # noqa: N803
    """
    Plot how many hours per day did I get more than 100Wh.
    """
    file_name = f"hours-of-{Wh_target}Wh"
    print("plot", file_name)

    fig, ax = plt.subplots()
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
    df_hour = prepare_df_hours(read_database())
    MAX_KWH_PER_HOUR = df_hour["kWh"].max()
    plot_kWh(df_hour, "hour", kWh_max=MAX_KWH_PER_HOUR)

    # how many hours per day did I reach a certain kWh target
    df_hours_of_50W = prepare_df_hours_goal_reached(  # noqa: N816
        df_hour, kWh_target=50 / 1000
    )
    plot_hours_goal_reached(df_hours_of_50W, Wh_target=50)

    df_hours_of_100W = prepare_df_hours_goal_reached(  # noqa: N816
        df_hour, kWh_target=100 / 1000
    )
    plot_hours_goal_reached(df_hours_of_100W, Wh_target=100)

    df_hours_of_200W = prepare_df_hours_goal_reached(  # noqa: N816
        df_hour, kWh_target=200 / 1000
    )
    plot_hours_goal_reached(df_hours_of_200W, Wh_target=200)

    df_hour_14d = prepare_df_last_14_days(df_hour)
    plot_last_14_days(df_hour_14d)

    df_day = prepare_df_day(df_hour)
    kWh_sum = int(round(df_day["kWh"].sum(), 0))  # noqa: N816
    MAX_KWH_PER_DAY = ceil(df_day["kWh"].max())

    df_week = prepare_df_week(df_day)
    df_month = prepare_df_month(df_day)

    # add last values
    today = pd.Timestamp.now().normalize()
    for df in (df_day, df_week, df_month):
        last_values = df.iloc[-1]
        df.loc[today] = last_values  # type: ignore

    plot_kWh(df_day, "day", kWh_sum=kWh_sum, kWh_max=MAX_KWH_PER_DAY)
    plot_kWh(df_week, "week", kWh_sum=kWh_sum)
    plot_kWh(df_month, "month", kWh_sum=kWh_sum)

    plot_kWh_date_mean(df_day, df_week, df_month, kWh_sum=kWh_sum)

    Path("out").mkdir(exist_ok=True)
    df_day["kWh"].round(3).to_csv("out/day.csv")
    df_week[["kWh_sum", "kWh_mean"]].round(3).to_csv("out/week.csv")
    df_month[["kWh_sum", "kWh_mean"]].round(3).to_csv("out/month.csv")
