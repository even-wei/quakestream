import pandas as pd

from . import constants
from dagster import asset, MetadataValue, MaterializeResult

@asset(
    group_name='raw_files',
)
def cwa_tw_earthquake_summary():
    """
      The raw earthquack summary data in Parquet, converted from CSV, sourced from cwa.govb.tw
    """

    num_rows = 0

    month_to_process = '2024-04'
    csv_file_path = constants.CWA_TW_SUMMARY_RAW_FILE_PATH.format(month_to_process)

    df = pd.read_csv(csv_file_path, skiprows=1, encoding='Big5')
    num_rows += len(df.index)

    df.to_parquet(constants.CWA_TW_SUMMARY_TEMPLATE_FILE_PATH.format(month_to_process))

    return MaterializeResult(
        metadata={
            'Number of records': MetadataValue.int(num_rows)
        }
    )
