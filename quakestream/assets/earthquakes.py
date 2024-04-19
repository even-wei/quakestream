import pandas as pd

from . import constants
from ..partitions import monthly_partition

from dagster import asset, MetadataValue, MaterializeResult
from dagster_duckdb import DuckDBResource

@asset(
    partitions_def=monthly_partition,
    group_name='raw_files',
)
def cwa_tw_earthquakes_file(context):
    """
      The raw earthquakes dataset in Parquet, converted from CSV, sourced from cwa.govb.tw
    """

    partition_date_str = context.asset_partition_key_for_output()
    month_to_fetch = partition_date_str[:-3]
    csv_file_path = constants.CWA_TW_EARTHQUAKES_RAW_FILE_PATH.format(month_to_fetch)

    df = pd.read_csv(csv_file_path, skiprows=1, encoding='Big5')
    num_rows = len(df.index)

    df.to_parquet(constants.CWA_TW_EARTHQUAKES_PARQUET_FILE_PATH.format(month_to_fetch))

    return MaterializeResult(
        metadata={
            'Number of records': MetadataValue.int(num_rows)
        }
    )

@asset(
    deps=["cwa_tw_earthquakes_file"],
    partitions_def=monthly_partition,
    group_name='ingested',
)
def cwa_tw_earthquakes(context, database: DuckDBResource):
    """
      The raw earthquakes dataset, loaded into a DuckDB database
    """

    partition_date_str = context.asset_partition_key_for_output()
    month_to_fetch = partition_date_str[:-3]

    sql_query = f"""
        create table if not exists earthquakes (
          no varchar, origin_time timestamp,
          latitude double, longtitude double,
          magnitude double, focal_depth double,
          location varchar, partition_date varchar
        );

        delete from earthquakes where partition_date = '{month_to_fetch}';

        insert into earthquakes
        select
          編號 as no,
          地震時間 as origin_time,
          經度 as latitude,
          緯度 as longtitude,
          規模 as magnitude,
          深度 as focal_depth,
          位置 as location,
          '{month_to_fetch}' as partition_date
        from '{constants.CWA_TW_EARTHQUAKES_PARQUET_FILE_PATH.format(month_to_fetch)}';
    """

    with database.get_connection() as conn:
        conn.execute(sql_query)
