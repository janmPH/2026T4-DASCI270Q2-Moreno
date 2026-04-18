from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    load_asset_checks_from_modules,
)

from pipeline.resources.sales_io import SalesIOResource
from pipeline.assets import sales_assets
from pipeline.checks import sales_checks

# Load all assets and checks from their modules
all_assets = load_assets_from_modules([sales_assets])
all_checks = load_asset_checks_from_modules([sales_checks])

# Define a job that runs all assets
daily_sales_job = define_asset_job(name="daily_sales_job", selection="*")

# I chose a schedule over a sensor because this is a predictable
# daily pipeline — sensors are better for event-driven triggers
# (e.g., "run when a new file appears"). A midnight cron schedule
# is simpler, more predictable, and easier to monitor for this use case.
daily_schedule = ScheduleDefinition(
    job=daily_sales_job,
    cron_schedule="0 0 * * *",
    name="daily_sales_schedule",
)

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    schedules=[daily_schedule],
    jobs=[daily_sales_job],
    resources={
        "sales_io": SalesIOResource(csv_path="sales_data.csv"),
    },
)
