from dagster import ConfigurableResource
import pandas as pd


class SalesIOResource(ConfigurableResource):
    """
    A configurable resource that handles reading sales data.
    By making this a resource, we can swap out the file path
    without changing any asset logic — this is dependency injection.
    """
    csv_path: str = "sales_data.csv"

    def load_raw_data(self) -> pd.DataFrame:
        """Reads the CSV from the configured path."""
        return pd.read_csv(self.csv_path)