import pandas as pd
import logging
import os
import json
from pathlib import Path
from datetime import datetime

# Get the absolute path to the project root directory
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger(__name__)

# Define data directories using the base path
RAW_BASE = Path(os.path.join(BASE_PATH, "data", "raw"))
STAGING_BASE = Path(os.path.join(BASE_PATH, "data", "staging"))
CONFIG_PATH = Path(os.path.join(BASE_PATH, "metadata", "data_config.json"))

def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)

def parse_csv(path):
    return pd.read_csv(path)
    
def parse_json(path):
    return pd.read_json(path)
    
def parse_xml(path, **kwargs):
    return pd.read_xml(path)

def parse_file(file_path, file_type, **kwargs):
    parsers = {
        "csv": parse_csv,
        "json": parse_json,
        "xml": parse_xml
    }
    return parsers[file_type](file_path)
    
def ingest_files():
    config = load_config()
    raw_files = config["raw_files"]
    
    logger.info(f"Available data types: {list(raw_files.keys())}")
    for data_type, file_info in raw_files.items():
        file_path = RAW_BASE / file_info["filename"]
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            continue

        logger.info(f"Processing {data_type} file: {file_path} with type {file_info['type']}")
        
        # Parse file based on type with any additional parameters
        kwargs = {}
        if file_info["type"] == "xml" and "record_tag" in file_info:
            kwargs["record_tag"] = file_info["record_tag"]
        
        try:
            df = parse_file(file_path, file_info["type"], **kwargs)
            
            # Process the data based on type
            logger.info(f"Processing data_type: '{data_type}'")
            if data_type == "sales":  # This matches the key in raw_files from config
                # Print column names to debug
                logger.info(f"Available columns in sales data: {df.columns.tolist()}")
                
                # Try to find date and region columns
                date_columns = [col for col in df.columns if 'date' in col.lower()]
                region_columns = [col for col in df.columns if 'region' in col.lower()]
                
                if date_columns and region_columns:
                    date_col = date_columns[0]
                    region_col = region_columns[0]
                    
                    df[date_col] = pd.to_datetime(df[date_col]).dt.date
                    df[region_col] = df[region_col].astype(str)
                    
                    # Partition sales data by date and region
                    for (sale_date, region), part in df.groupby([date_col, region_col]):
                        part_dir = STAGING_BASE / f"{sale_date.isoformat()}" / f"region_{region}"
                        part_dir.mkdir(parents=True, exist_ok=True)
                        out_file = part_dir / f"{data_type}_{datetime.utcnow().strftime('%H%M%S')}.parquet"
                        part.to_parquet(out_file, index=False)
                        logger.info(f"Wrote partition {out_file}")
                else:
                    logger.error(f"Could not find date and region columns. Available columns: {df.columns.tolist()}")
            else:
                # For other data types (region and product), save directly to staging
                out_file = STAGING_BASE / f"{data_type}.parquet"
                df.to_parquet(out_file, index=False)
                logger.info(f"Wrote {data_type} data to {out_file}")
                
        except Exception as e:
            logger.error(f"Error processing {data_type} file: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_files()