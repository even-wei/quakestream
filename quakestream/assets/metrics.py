import pickle

from sqlglot import select, condition
from dagster import asset

from dagster_duckdb import DuckDBResource

@asset(
    deps=['cwa_tw_earthquakes'],
    group_name='metrics'
)
def earthquakes_by_day_and_magnitude(database: DuckDBResource):
    """
      The number of earthquakes from the past 30 days, aggregated by day and magnitude.
    """

    query = select(
        'date_trunc(day, origin_time) as day_date',
        'magnitude',
        'count(*) as count'
    ).from_(
        'earthquakes'
    ).where(
        condition("origin_time >= current_date - 30")
    ).group_by(
        'day_date',
        'magnitude'
    ).order_by(
        'day_date',
        'magnitude'
    ).sql()

    with database.get_connection() as conn:
        df = conn.execute(query).fetch_df()

    return pickle.dumps(df)

@asset(
    deps=['cwa_tw_earthquakes'],
    group_name='metrics'
)
def earthquakes_by_month_and_magnitude(database: DuckDBResource):
    """
      The number of earthquakes per month, aggregated by month and magnitude.
    """

    query = select(
        'date_trunc(month, origin_time) as month_date',
        'magnitude',
        'count(*) as count'
    ).from_(
        'earthquakes'
    ).group_by(
        'month_date',
        'magnitude'
    ).order_by(
        'month_date',
        'magnitude'
    ).sql()

    with database.get_connection() as conn:
        df = conn.execute(query).fetch_df()

    return pickle.dumps(df)
