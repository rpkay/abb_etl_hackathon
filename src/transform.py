import pandas as pd
import glob
import os
from pathlib import Path
import hashlib
import sqlalchemy
from sqlalchemy import create_engine
from typing import Dict
from dotenv import load_dotenv

from utils import log_etl_metadata

# Set up base paths
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_BASE = Path(os.path.join(BASE_PATH, "data", "raw"))
STAGING_BASE = Path(os.path.join(BASE_PATH, "data", "staging"))
WAREHOUSE_BASE = Path(os.path.join(BASE_PATH, "data", "warehouse"))

# Ensure warehouse directory exists
WAREHOUSE_BASE.mkdir(parents=True, exist_ok=True)




def ingest_to_postgres(
    tables: Dict[str, pd.DataFrame],
    schema: str = "public",
    if_exists: str = "append"
):
    """
    Ingest multiple pandas DataFrames into PostgreSQL and log ETL metadata.
    """

    load_dotenv()

    db_name = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")

    if not all([db_name, user, password]):
        raise ValueError("❌ Missing required PostgreSQL credentials in .env file")

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(conn_str)

    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

        for table_name, df in tables.items():
            print(f"📦 Loading table '{table_name}' ({len(df)} rows) ...")
            try:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=schema,
                    if_exists=if_exists,
                    index=False
                )
                # ✅ Log success
                log_etl_metadata(
                    schema_name=schema,
                    table_name=f"{schema}.{table_name}",
                    status="SUCCESS",
                    record_count=len(df),
                    remarks="Loaded successfully"
                )
            except Exception as e:
                # ❌ Log failure
                log_etl_metadata(
                    schema_name=schema,
                    table_name=f"{schema}.{table_name}",
                    status="FAILED",
                    record_count=0,
                    remarks=str(e)
                )
                print(f"❌ Error loading table {table_name}: {e}")

    print("✅ All tables ingested and metadata logged.")


def generate_surrogate_key(series: pd.Series) -> pd.Series:
    """
    Generate deterministic surrogate keys using MD5 hash of the input series.
    Returns a numeric surrogate key (int64) derived from the hash.
    """
    return series.astype(str).apply(lambda x: int(hashlib.md5(x.encode()).hexdigest(), 16) % (10**12))


# ===============================
# 1️⃣ Load Dimension Functions
# ===============================

def load_dim_product(staging_path: Path) -> pd.DataFrame:
    """Load product dimension from Parquet file."""
    product_path = staging_path / "product.parquet"
    dim_product = pd.read_parquet(product_path)
    dim_product["product_key"] = generate_surrogate_key(
        dim_product["product_name"] + "_" + dim_product["manufacturer"]
    )
    dim_product = dim_product.astype({
        "product_key": "int64",
        "product_name": "string",
        "category": "string",
        "manufacturer": "string",
        "warranty_years": "int64",
    })
    dim_product["start_date"] =  pd.Timestamp.today()
    dim_product["end_date"] = pd.Timestamp("2099-12-31")
    dim_product["is_current"] = True
    print(f"✅ dim_product loaded: {dim_product.shape}")
    return dim_product


def load_dim_region(staging_path: Path) -> pd.DataFrame:
    """Load region dimension from Parquet file."""
    region_path = staging_path / "region.parquet"
    dim_region = pd.read_parquet(region_path)
    dim_region["region_key"] = generate_surrogate_key(dim_region["Name"])
    dim_region.rename(columns={"Name": "region_name", "Manager":"manager", "EstablishedYear":"established_year"}, inplace=True)
    dim_region = dim_region.astype({
        "region_name": "string",
        "manager": "string",
        "established_year": "int64"
    })
    print(f"✅ dim_region loaded: {dim_region.shape}")
    return dim_region


# ===============================
# 2️⃣ Load Fact Table Function
# ===============================

def load_fact_sales(staging_path: Path) -> pd.DataFrame:
    """
    Reads partitioned sales data from folders structured as:
      staging/sales/<date>/region_*/sales.parquet
    """
    sales_path = staging_path / "sales"
    # Pattern: find all parquet files recursively
    sales_files = glob.glob(str(sales_path / "*" / "region_*" / "sales*.parquet"))

    sales_df_list = []

    for file_path in sales_files:

        df = pd.read_parquet(file_path)
        sales_df_list.append(df)

    fact_sales = pd.concat(sales_df_list, ignore_index=True)
    print(f"✅ fact_sales raw loaded from {len(sales_files)} files, shape: {fact_sales.shape}")
    fact_sales.rename(columns={"region": "region_name"}, inplace=True)
    return fact_sales


# ===============================
# 3️⃣ Build Date Dimension
# ===============================

def build_dim_date(fact_sales: pd.DataFrame) -> pd.DataFrame:
    """Create date dimension from sale_date in fact_sales."""
    fact_sales["date"] = pd.to_datetime(fact_sales["date"], errors="coerce")
    unique_dates = sorted(fact_sales["date"].unique())
    dim_date = pd.DataFrame({
        "date_key": range(1, len(unique_dates) + 1),
        "full_date": unique_dates
    })
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.strftime("%B")
    dim_date["quarter"] = "Q" + dim_date["full_date"].dt.quarter.astype(str)
    print(f"✅ dim_date created: {dim_date.shape}")
    return dim_date


# ===============================
# 4️⃣ Transform Fact Table
# ===============================

def transform_fact_sales(fact_sales: pd.DataFrame, dim_region: pd.DataFrame, dim_date: pd.DataFrame, dim_product:pd.DataFrame) -> pd.DataFrame:
    """Join fact_sales with region and date dimension to build final schema."""
    # Add region_key from dim_region
    fact_sales = fact_sales.merge(dim_region[["region_key", "region_name"]], on="region_name", how="left")

    # Add date_key from dim_date
    fact_sales = fact_sales.merge(dim_date[["date_key", "full_date"]], left_on="date", right_on="full_date", how="left")
    
    #Add product_key from dim_product
    fact_sales = fact_sales.merge(dim_product[["product_key", "product_name"]], left_on="product", right_on="product_name", how="left")

    # Select only required columns
    fact_sales = fact_sales[[
        "product_key", "region_key", "date_key", "quantity", "price", "total"
    ]].reset_index(drop=True)
    
    print(f"✅ fact_sales transformed: {fact_sales.shape}")
    return fact_sales


# ===============================
# 5️⃣ Main Orchestrator
# ===============================

def build_star_schema():
    """Main function to read staging data and build star schema."""
    print("🚀 Starting star schema build process...")

    # Load and transform dimensions and facts
    dim_product = load_dim_product(STAGING_BASE)
    dim_region = load_dim_region(STAGING_BASE)
    fact_sales_raw = load_fact_sales(STAGING_BASE)
    dim_date = build_dim_date(fact_sales_raw)
    fact_sales = transform_fact_sales(fact_sales_raw, dim_region, dim_date, dim_product)


    # Save to warehouse
    tables = {
        "dim_product": dim_product,
        "dim_region": dim_region,
        "dim_date": dim_date,
        "fact_sales": fact_sales
    }
    
    return tables


# ===============================
# 6️⃣ Example Usage
# ===============================
if __name__ == "__main__":
    dfs = build_star_schema()
    ingest_to_postgres(dfs, schema="retail_dw", if_exists="append")
