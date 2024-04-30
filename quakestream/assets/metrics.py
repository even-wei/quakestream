from sqlglot import select
from dagster import asset

from dagster_duckdb import DuckDBResource

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
        'magnitude desc'
    ).sql()

    with database.get_connection() as conn:
        df = conn.execute(query).fetch_df()

    return df
