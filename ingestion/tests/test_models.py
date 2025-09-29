from ingestion.bigquery import build_pypi_query
from ingestion.models import PypiJobParameters


def test_build_pypi_query():
    params = PypiJobParameters(
        table_name="test_table",
        s3_path="s3://bucket/path",
        aws_profile="test_profile",
        gcp_project="test_project",
        timestamp_column="timestamp",
        start_date="2020-01-01",
        end_date="2020-01-02",
        database_name="test_database",
        pypi_project="duckdb",
    )
    query = build_pypi_query(params)
    expected = f"""
    SELECT *
    FROM
        `bigquery-public-data.pypi.file_downloads`
    WHERE
        project = 'duckdb'
        AND timestamp >= TIMESTAMP("2020-01-01")
        AND timestamp < TIMESTAMP("2020-01-02")
    """
    assert query.strip().replace("\t", "") == expected.strip().replace("\t", "")
