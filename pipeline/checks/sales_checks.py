import pandas as pd
from dagster import asset_check, AssetCheckResult


@asset_check(asset="clean_sales")
def check_no_nulls_in_clean_sales(clean_sales: pd.DataFrame) -> AssetCheckResult:
    """
    DATA QUALITY CHECK for clean_sales.
    Verifies that no null values exist in critical columns
    after the cleaning step.
    """
    critical_cols = ["order_id", "customer_id", "region", "sales_amount"]
    null_counts = clean_sales[critical_cols].isnull().sum()
    has_nulls = bool(null_counts.sum() > 0)

    return AssetCheckResult(
        passed=not has_nulls,
        metadata={
            "null_counts": str(null_counts.to_dict()),
            "rows_checked": len(clean_sales),
        },
    )


@asset_check(asset="daily_metrics")
def check_daily_metrics_business_rules(daily_metrics: dict) -> AssetCheckResult:
    """
    BUSINESS RULE VALIDATION for daily_metrics.
    Ensures total sales is positive and total orders is at least 1.
    """
    total_sales = daily_metrics.get("total_sales", 0)
    total_orders = daily_metrics.get("total_orders", 0)

    passed = total_sales > 0 and total_orders > 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_sales": total_sales,
            "total_orders": total_orders,
            "top_region": daily_metrics.get("top_region", "N/A"),
        },
    )
