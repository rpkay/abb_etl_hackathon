from flask import Flask, jsonify
from flasgger import Swagger
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

app = Flask(__name__)
swagger = Swagger(app)

# Database config
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT", 5432)
}

def get_connection():
    """Create a PostgreSQL connection."""
    return psycopg2.connect(**DB_CONFIG)

# ----------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------

@app.route("/schemas", methods=["GET"])
def get_table_schemas():
    """
    Get Table Schemas
    ---
    tags:
      - Metadata
    description: Retrieve all tables and their columns from the public schema.
    responses:
      200:
        description: A list of tables and their column definitions.
        examples:
          application/json: [{"table_name": "fact_sales", "column_name": "sale_id", "data_type": "integer"}]
    """
    query = """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'retail_dw'
        ORDER BY table_name, ordinal_position;
    """
    with get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return jsonify(rows)


@app.route("/load_status", methods=["GET"])
def get_load_status():
    """
    Get ETL Load Status
    ---
    tags:
      - Metadata
    description: Retrieve ETL load status from the etl_metadata table.
    responses:
      200:
        description: List of tables and their load statuses.
        examples:
          application/json: [{"table_name": "fact_sales", "load_status": "SUCCESS"}]
    """
    query = "SELECT table_name, load_status FROM retail_dw.etl_metadata;"
    with get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return jsonify(rows)


@app.route("/last_updated", methods=["GET"])
def get_last_updated():
    """
    Get Last Updated Timestamps
    ---
    tags:
      - Metadata
    description: Retrieve last load timestamps and record counts for all tables.
    responses:
      200:
        description: A list of tables with last update timestamps and record counts.
        examples:
          application/json: [{"table_name": "dim_product", "last_load_timestamp": "2025-10-30T10:00:00", "record_count": 2500}]
    """
    query = "SELECT table_name, last_load_timestamp, record_count FROM retail_dw.etl_metadata;"
    with get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return jsonify(rows)

# ----------------------------------------------------------------
# Run Flask App
# ----------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, port=5000)
