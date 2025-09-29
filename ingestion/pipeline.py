import os
import duckdb
import fire
from loguru import logger
from ingestion.bigquery import (
    build_pypi_query,
    get_bigquery_client,
    get_bigquery_result,
)
from ingestion.duck import (
    connect_to_md,
    create_table_from_dataframe,
    load_aws_credentials,
    write_duckdb_to_s3,
    write_to_md_from_duckdb,
)
from ingestion.models import FileDownloads, PypiJobParameters, validate_table


def main(params: PypiJobParameters):
    df = get_bigquery_result(
        build_pypi_query(params),
        get_bigquery_client(params.gcp_project),
        model=FileDownloads,
    )

    validate_table(df, FileDownloads)

    # Load to DuckDB
    conn = duckdb.connect()
    create_table_from_dataframe(conn, params.table_name, df)

    logger.info(f"Sinking data to {params.destination}")
    if "local" in params.destination:
        conn.sql(
            f"COPY {params.table_name} TO '{params.table_name}.csv' (FORMAT CSV, HEADER TRUE);"
        )
    if "s3" in params.destination:
        load_aws_credentials(conn)
        write_duckdb_to_s3(
            conn, params.table_name, params.s3_path, params.timestamp_column
        )
    if "md" in params.destination:
        connect_to_md(conn)
        write_to_md_from_duckdb(
            duckdb_con=conn,
            table=f"{params.table_name}",
            local_database='memory',
            remote_database=f'{params.pypi_project}_pypi',
            start_date=params.start_date,
            end_date=params.end_date,
            timestamp_column=params.timestamp_column,
        )


if __name__ == "__main__":
    fire.Fire(lambda **kwargs: main(PypiJobParameters(**kwargs)))
