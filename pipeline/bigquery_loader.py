"""
Google BigQuery data loader with streaming inserts and schema management.
"""

from typing import Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import structlog

logger = structlog.get_logger(__name__)


# Default schema for scraped data
SCRAPE_RESULTS_SCHEMA = [
    bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("status_code", "INTEGER"),
    bigquery.SchemaField("content_length", "INTEGER"),
    bigquery.SchemaField("scraped_at", "TIMESTAMP"),
    bigquery.SchemaField("duration_ms", "FLOAT"),
    bigquery.SchemaField("error", "STRING"),
    bigquery.SchemaField(
        "metadata",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("key", "STRING"),
            bigquery.SchemaField("value", "STRING"),
        ],
    ),
]


class BigQueryLoader:
    """
    BigQuery data loader with streaming inserts, schema validation,
    and automatic table creation.

    Example:
        loader = BigQueryLoader(project_id="my-project", dataset_id="pipeline_data")
        loader.insert_rows("scrape_results", rows)
    """

    def __init__(self, project_id: str, dataset_id: str, location: str = "EU"):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.location = location
        self.client = bigquery.Client(project=project_id)
        self.logger = structlog.get_logger(self.__class__.__name__)

    def _table_ref(self, table_id: str) -> str:
        return f"{self.project_id}.{self.dataset_id}.{table_id}"

    def ensure_dataset_exists(self) -> None:
        """Create dataset if it does not exist."""
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        try:
            self.client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.location
            self.client.create_dataset(dataset)
            self.logger.info("dataset_created", dataset=dataset_ref)

    def ensure_table_exists(
        self,
        table_id: str,
        schema: list[bigquery.SchemaField],
        partition_field: Optional[str] = "scraped_at",
    ) -> None:
        """Create table with schema if it does not exist."""
        table_ref = self._table_ref(table_id)
        try:
            self.client.get_table(table_ref)
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)

            if partition_field:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field,
                )

            self.client.create_table(table)
            self.logger.info("table_created", table=table_ref)

    def insert_rows(
        self,
        table_id: str,
        rows: list[dict],
        skip_invalid_rows: bool = False,
    ) -> list[dict]:
        """
        Stream rows into BigQuery using the streaming insert API.

        Args:
            table_id: Target table ID
            rows: List of row dicts matching the table schema
            skip_invalid_rows: Skip rows that fail schema validation

        Returns:
            List of errors (empty if all rows inserted successfully)
        """
        if not rows:
            self.logger.warning("no_rows_to_insert", table=table_id)
            return []

        table_ref = self._table_ref(table_id)

        errors = self.client.insert_rows_json(
            table_ref,
            rows,
            skip_invalid_rows=skip_invalid_rows,
        )

        if errors:
            self.logger.error(
                "bigquery_insert_errors",
                table=table_id,
                error_count=len(errors),
                errors=errors[:5],  # Log first 5 errors only
            )
        else:
            self.logger.info(
                "rows_inserted",
                table=table_id,
                row_count=len(rows),
            )

        return errors

    def run_query(self, sql: str, params: Optional[list] = None) -> list[dict]:
        """
        Run a BigQuery SQL query and return results as list of dicts.

        Args:
            sql: SQL query string
            params: Optional list of bigquery.ScalarQueryParameter

        Returns:
            List of row dicts
        """
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])

        self.logger.info("running_query", sql=sql[:100])
        query_job = self.client.query(sql, job_config=job_config)
        results = query_job.result()

        rows = [dict(row) for row in results]
        self.logger.info("query_complete", row_count=len(rows))
        return rows

    def load_from_gcs(
        self,
        gcs_uri: str,
        table_id: str,
        schema: list[bigquery.SchemaField],
        source_format: bigquery.SourceFormat = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition: str = "WRITE_APPEND",
    ) -> bigquery.LoadJob:
        """
        Load data from GCS into BigQuery (batch load).
        More cost-effective than streaming for large datasets.
        """
        table_ref = self._table_ref(table_id)
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=source_format,
            write_disposition=write_disposition,
        )

        load_job = self.client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)

        self.logger.info(
            "gcs_load_started",
            gcs_uri=gcs_uri,
            table=table_id,
            job_id=load_job.job_id,
        )
        return load_job
