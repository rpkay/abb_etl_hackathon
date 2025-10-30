import psycopg2
from datetime import datetime
import os
from dotenv import load_dotenv

def log_etl_metadata(schema_name,table_name, status, record_count=0, remarks=None):
    """Insert or update ETL load metadata in PostgreSQL."""
    load_dotenv()

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
        )
        cur = conn.cursor()

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.etl_metadata (
                table_name VARCHAR(100) PRIMARY KEY,
                last_load_timestamp TIMESTAMP,
                load_status VARCHAR(20),
                record_count BIGINT,
                remarks TEXT
            );
        """)

        cur.execute(f"""
            INSERT INTO {schema_name}.etl_metadata (table_name, last_load_timestamp, load_status, record_count, remarks)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (table_name)
            DO UPDATE SET
                last_load_timestamp = EXCLUDED.last_load_timestamp,
                load_status = EXCLUDED.load_status,
                record_count = EXCLUDED.record_count,
                remarks = EXCLUDED.remarks;
        """, (table_name, datetime.now(), status, record_count, remarks))
        
        conn.commit()
        cur.close()
        print(f"🗂️ Metadata logged for table: {table_name}")
    except Exception as e:
        print(f"⚠️ Failed to log metadata for {table_name}: {e}")
    finally:
        if conn:
            conn.close()