from ..assets import constants
from dagster import MonthlyPartitionsDefinition

start_date = constants.START_DATE

monthly_partition = MonthlyPartitionsDefinition(
    start_date=start_date,
    end_offset=1,
)
