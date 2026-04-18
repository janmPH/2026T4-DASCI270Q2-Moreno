import pandas as pd
from dagster import asset, AssetExecutionContext
from pipeline.resources.sales_io import SalesIOResource


@asset
def raw_sales(
    context: AssetExecutionContext, sales_io: SalesIOResource
) -> pd.DataFrame:
    """
    Reads raw transactional sales data from a CSV file.
    Uses the sales_io resource so the file path is configurable.
    """
    df = sales_io.load_raw_data()
    context.log.info(f"Loaded {len(df)} raw records from CSV.")
    return df


@asset
def clean_sales(
    context: AssetExecutionContext, raw_sales: pd.DataFrame, sales_io: SalesIOResource
) -> pd.DataFrame:
    """
    Cleans the raw sales data:
    - Drops duplicate rows
    - Handles missing/invalid values
    - Adds a derived 'sales_amount' column (quantity * unit_price)
    """
    df = raw_sales.copy()

    # Step 1: Drop exact duplicate rows
    before = len(df)
    df = df.drop_duplicates()
    context.log.info(f"Dropped {before - len(df)} duplicate rows.")

    # Step 2: Drop rows where critical columns are null
    critical_cols = ["order_id", "customer_id", "region", "quantity", "unit_price"]
    df = df.dropna(subset=critical_cols)

    # Step 3: Remove rows with invalid negative or zero values
    df = df[df["quantity"] > 0]
    df = df[df["unit_price"] > 0]

    # Step 4: Add derived column
    df["sales_amount"] = df["quantity"] * df["unit_price"]

    context.log.info(f"Clean dataset has {len(df)} records.")
    return df


@asset
def sales_by_region(
    context: AssetExecutionContext, clean_sales: pd.DataFrame
) -> pd.DataFrame:
    """
    Groups clean sales data by region and computes:
    - total_sales: sum of all sales_amount per region
    - total_orders: count of orders per region
    """
    df = (
        clean_sales.groupby("region")
        .agg(total_sales=("sales_amount", "sum"), total_orders=("order_id", "count"))
        .reset_index()
    )

    context.log.info(f"Aggregated sales for {len(df)} regions.")
    return df


@asset
def daily_metrics(
    context: AssetExecutionContext,
    clean_sales: pd.DataFrame,
    sales_by_region: pd.DataFrame,
) -> dict:
    """
    Produces one daily summary dictionary with key KPIs.
    Uses clean_sales for customer-level metrics and
    sales_by_region for the top region lookup.
    """
    total_sales = clean_sales["sales_amount"].sum()
    total_orders = len(clean_sales)
    unique_customers = clean_sales["customer_id"].nunique()

    top_region_row = sales_by_region.loc[sales_by_region["total_sales"].idxmax()]
    top_region = top_region_row["region"]

    metrics = {
        "total_sales": round(float(total_sales), 2),
        "total_orders": int(total_orders),
        "unique_customers": int(unique_customers),
        "top_region": top_region,
    }

    context.log.info(f"Daily Metrics: {metrics}")
    return metrics
