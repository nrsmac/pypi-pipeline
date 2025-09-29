import os
import pandas as pd
from loguru import logger


def create_table_from_dataframe(duckdb_con, table_name: str, dataframe: pd.DataFrame):
    duckdb_con.sql(
        f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM dataframe
        """
    )


def load_aws_credentials(duckdb_con):
    logger.info(f"Loading AWS credentials from environment variables")
    query =f"""
    CREATE OR REPLACE SECRET secret (
        TYPE s3,
        PROVIDER config,
        KEY_ID '{os.environ.get("AWS_ACCESS_KEY_ID")}',
        SECRET '{os.environ.get("AWS_SECRET_ACCESS_KEY")}',
        SESSION_TOKEN '{os.environ.get("AWS_SESSION_TOKEN")}',
        REGION 'us-east-1'
    );
    """
    print(query)
    duckdb_con.sql(query)


def write_duckdb_to_s3(duckdb_con, table: str, s3_path: str, timestamp_column: str):
    logger.info(f"Writing DuckDB table {table} to S3 at {s3_path}")
    duckdb_con.sql(
        f"""
        COPY(
            SELECT *, 
            YEAR({timestamp_column}) AS year,
            MONTH({timestamp_column}) AS month
            FROM {table}
        ) TO '{s3_path}/{table}' 
            (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE 1, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
        """
    )


def connect_to_md(duckdb_con):
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        raise ValueError("Environment variable MOTHERDUCK_TOKEN is not set.")
    duckdb_con.sql(f"ATTACH 'md:';")


def write_to_md_from_duckdb(
    duckdb_con,
    table: str,
    local_database: str,
    remote_database: str,
    timestamp_column: str,
    start_date: str,
    end_date: str,
):
    logger.info(f"Creating remote database {remote_database} if it doesn't exist.")
    duckdb_con.sql(f"CREATE DATABASE IF NOT EXISTS {remote_database};")
    duckdb_con.sql(f"USE {remote_database};")
    logger.info(f"Writing DuckDB table {table} to MD at {remote_database}")
    duckdb_con.sql(
        f"CREATE TABLE IF NOT EXISTS {remote_database}.{table} AS SELECT * FROM {local_database}.{table};"
    )
    # Delete existing data in date range
    duckdb_con.sql(
        f"DELETE FROM {remote_database}.{table} WHERE {timestamp_column} BETWEEN '{start_date}' AND '{end_date}';"
    )
    # Insert new data
    duckdb_con.sql(
        f"""
        INSERT INTO {remote_database}.{table}
        SELECT * FROM {local_database}.{table}
        """
    )
